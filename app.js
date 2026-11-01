/* 心有灵犀 · 双人实时联想游戏
 *
 * 同步模型（无需自建后端）：
 *   每个房间对应 MQTT 主题  <namespace>/<hash>/...
 *     cfg        房间配置（玩家、限时、阶段、本局 id），任何玩家可写，版本号 + pid 决胜
 *     p/<pid>    每位玩家自己的状态（昵称、本局写过的词），只有本人写，seq 递增
 *     on/<pid>   在线状态 "1"/"0"（遗嘱消息自动置 0）
 *   cfg 与 p/* 都是 retained 消息，刷新/重连后可立即恢复；内容用 AES-GCM 加密。
 *   同时连接多个服务器，消息按版本去重，任意一条线路通即可。
 */
(() => {
  'use strict';

  // ---------- 常量 ----------
  const CONF = window.LINGXI_CONFIG || {};
  const params = new URLSearchParams(location.search);
  const BROKERS = (params.get('broker') || '').split(',').map(s => s.trim()).filter(Boolean);
  if (!BROKERS.length) BROKERS.push(...(CONF.brokers || ['wss://broker.emqx.io:8084/mqtt']));
  const NS = CONF.namespace || 'lingxi/v1';
  const CODE_ALPHABET = '23456789ABCDEFGHJKMNPQRSTUVWXYZ';
  const CODE_LEN = 6;
  const MAX_WORD = 20;
  const ROUND_GRACE = 900;     // 揭晓动画后再开始计时（毫秒）
  const SYNC_WAIT = 1600;      // 订阅后等待保留消息的时间

  // ---------- 工具 ----------
  const $ = (s, el = document) => el.querySelector(s);
  const $$ = (s, el = document) => Array.from(el.querySelectorAll(s));
  const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const rand = (n = 10) => Array.from(crypto.getRandomValues(new Uint8Array(n)), x => (x % 36).toString(36)).join('');
  const newCode = () => Array.from(crypto.getRandomValues(new Uint8Array(CODE_LEN)), x => CODE_ALPHABET[x % CODE_ALPHABET.length]).join('');
  const cleanCode = s => String(s || '').toUpperCase().replace(/[^0-9A-Z]/g, '');
  const validCode = c => c.length === CODE_LEN && [...c].every(ch => CODE_ALPHABET.includes(ch));
  const norm = s => String(s || '').normalize('NFKC').toLowerCase().replace(/[\s\p{P}\p{S}]/gu, '');
  const same = (a, b) => { const x = norm(a); return x !== '' && x === norm(b); };
  const initial = name => (Array.from(String(name || '?').trim())[0] || '?').toUpperCase();
  const isTouch = matchMedia('(pointer: coarse)').matches;

  const store = {
    get(k, d = null) { try { const v = localStorage.getItem('lx:' + k); return v == null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem('lx:' + k, JSON.stringify(v)); } catch { } },
    del(k) { try { localStorage.removeItem('lx:' + k); } catch { } },
  };

  // ---------- 状态 ----------
  const S = {
    gen: 0,            // 每次进出房间 +1，用于丢弃过期的异步结果
    code: null,        // 房间号
    room: null,        // { key, topic }
    me: null,          // { pid, name, spectator }
    mine: null,        // 我的玩家状态 { pid, name, gid, words, seq }
    cfg: null,         // 房间配置
    players: {},       // pid -> 玩家状态
    presence: {},      // pid -> { brokerIndex: bool }
    net: null,         // { base, clients }
    mode: null,        // create | join | resume
    ready: false,      // 首次同步完成
    readyAt: 0,
    notFound: false,
    seatPick: false,
    resultFor: null,   // 已弹出结果的局 id
    autoLock: null,    // 防止超时重复提交
    seen: {},          // brokerIndex -> { cfg, mine }：该服务器上最近看到的版本，用于修复过期的保留消息
  };

  // ---------- 加密 ----------
  const te = new TextEncoder(), td = new TextDecoder();
  const b64 = u8 => { let s = ''; for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]); return btoa(s); };
  const unb64 = s => Uint8Array.from(atob(s), c => c.charCodeAt(0));
  const hex = u8 => Array.from(u8, x => x.toString(16).padStart(2, '0')).join('');

  async function deriveRoom(code) {
    const base = await crypto.subtle.importKey('raw', te.encode(code), 'PBKDF2', false, ['deriveBits']);
    const bits = new Uint8Array(await crypto.subtle.deriveBits(
      { name: 'PBKDF2', salt: te.encode('lingxi-room-v1'), iterations: 100000, hash: 'SHA-256' }, base, 384));
    const key = await crypto.subtle.importKey('raw', bits.slice(0, 32), 'AES-GCM', false, ['encrypt', 'decrypt']);
    return { key, topic: hex(bits.slice(32, 48)) };
  }
  async function seal(obj, key) {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, te.encode(JSON.stringify(obj))));
    const out = new Uint8Array(12 + ct.length); out.set(iv); out.set(ct, 12);
    return b64(out);
  }
  async function unseal(str, key) {
    const u = unb64(str);
    const pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: u.slice(0, 12) }, key, u.slice(12));
    return JSON.parse(td.decode(pt));
  }

  // ---------- 网络 ----------
  function connectNet() {
    const base = `${NS}/${S.room.topic}`;
    const gen = S.gen;
    const pid = S.me.pid;
    const net = { base, clients: [], subscribed: false };
    S.net = net;
    BROKERS.forEach((url, i) => {
      const opts = {
        clientId: 'lx_' + rand(14),
        keepalive: 25,
        reconnectPeriod: 3000,
        connectTimeout: 12000,
        clean: true,
        resubscribe: true,
        will: { topic: `${base}/on/${pid}`, payload: '0', qos: 0, retain: true },
      };
      let c;
      try { c = mqtt.connect(url, opts); } catch (e) { console.warn('connect failed', url, e); return; }
      c.on('connect', () => {
        if (gen !== S.gen) return;
        S.seen[i] = {};
        c.subscribe(`${base}/#`, { qos: 1 }, err => {
          if (err || gen !== S.gen) return;
          if (!net.subscribed) { net.subscribed = true; setTimeout(() => gen === S.gen && finishSync(), SYNC_WAIT); }
          setTimeout(() => gen === S.gen && repair(i), SYNC_WAIT + 150);
        });
        if (S.me && !S.me.spectator) rawPub('on/' + pid, '1', true, 0, c);
        updateConn();
      });
      c.on('message', (topic, payload) => {
        if (gen !== S.gen) return;
        if (!topic.startsWith(base + '/')) return;
        handleMsg(topic.slice(base.length + 1), payload.toString(), i, gen);
      });
      for (const ev of ['close', 'offline', 'reconnect', 'end']) c.on(ev, () => {
        if (gen !== S.gen) return;
        // 这条线路断了：它报告的在线状态不再可信
        if (ev !== 'reconnect') for (const pid in S.presence) delete S.presence[pid][i];
        updateConn();
        render();
      });
      c.on('error', e => console.warn('[mqtt]', url, e && e.message));
      net.clients[i] = c;
    });
    // 一条线路都连不上时提示
    setTimeout(() => { if (gen === S.gen && !anyConnected()) toast('暂时连不上服务器，正在重试…'); }, 9000);
  }

  function closeNet() {
    const net = S.net; S.net = null;
    if (!net) return;
    for (const c of net.clients.filter(Boolean)) {
      try {
        if (S.me && !S.me.spectator && c.connected) c.publish(`${net.base}/on/${S.me.pid}`, '0', { qos: 0, retain: true });
        c.end(false);
      } catch { }
    }
  }

  const anyConnected = () => !!S.net && S.net.clients.some(c => c && c.connected);

  function rawPub(sub, payload, retain = true, qos = 1, only = null) {
    if (!S.net) return;
    for (const c of (only ? [only] : S.net.clients.filter(Boolean))) {
      try { c.publish(`${S.net.base}/${sub}`, payload, { retain, qos }); } catch (e) { console.warn(e); }
    }
  }

  // 串行加密 + 发布，保证服务器上保留的一定是最新版本
  let pubChain = Promise.resolve();
  function enqueuePub(sub, getObj, only = null) {
    const gen = S.gen, key = S.room && S.room.key;
    pubChain = pubChain.then(async () => {
      if (gen !== S.gen || !key) return;
      const obj = getObj();
      if (!obj) return;
      const payload = await seal(obj, key);
      if (gen !== S.gen) return;
      rawPub(sub, payload, true, 1, only);
    }).catch(e => console.warn(e));
  }
  function publishMine() {
    if (!S.mine || S.me.spectator) return;
    enqueuePub('p/' + S.mine.pid, () => S.mine);
  }
  function publishCfg() {
    if (!S.cfg) return;
    enqueuePub('cfg', () => S.cfg);
  }

  // 某个服务器上保留的数据比我手里的旧（或丢失了）时，补发最新版本
  function repair(i) {
    if (!S.net || !S.me || S.me.spectator) return;
    const c = S.net.clients[i];
    if (!c || !c.connected) return;
    const seen = S.seen[i] || {};
    if (S.cfg && S.cfg.players.includes(S.me.pid) && (!seen.cfg || cfgNewer(S.cfg, seen.cfg))) enqueuePub('cfg', () => S.cfg, c);
    if (S.mine && (seen.mine == null || S.mine.seq > seen.mine)) enqueuePub('p/' + S.mine.pid, () => S.mine, c);
  }
  const repairTimers = {};
  function repairSoon(i) {
    clearTimeout(repairTimers[i]);
    const gen = S.gen;
    repairTimers[i] = setTimeout(() => gen === S.gen && repair(i), 700);
  }

  async function handleMsg(sub, payload, bi, gen) {
    if (sub.startsWith('on/')) {
      const pid = sub.slice(3);
      (S.presence[pid] ||= {})[bi] = payload === '1';
      render();
      return;
    }
    if (!payload) return;
    let d;
    try { d = await unseal(payload, S.room.key); } catch { return; }
    if (gen !== S.gen) return;

    if (sub === 'cfg') {
      if (!d || !Array.isArray(d.players)) return;
      (S.seen[bi] ||= {}).cfg = d;
      if (cfgNewer(d, S.cfg)) applyCfg(d, false);
      else if (S.cfg && cfgNewer(S.cfg, d)) repairSoon(bi);
    } else if (sub.startsWith('p/')) {
      const pid = sub.slice(2);
      if (!d || d.pid !== pid || !Array.isArray(d.words)) return;
      if (S.mine && pid === S.mine.pid) {
        (S.seen[bi] ||= {}).mine = d.seq;
        if (d.seq < S.mine.seq) repairSoon(bi);
      }
      const cur = S.players[pid];
      if (cur && d.seq <= cur.seq) return;
      const prevName = cur && cur.name;
      S.players[pid] = d;
      if (S.mine && pid === S.mine.pid && d.seq > S.mine.seq) { S.mine = { ...d }; saveMine(); }
      if (S.ready && S.cfg && pid !== (S.me && S.me.pid) && S.cfg.players.includes(pid) && !prevName && d.name) {
        toast(`${d.name} 来了`);
      }
      render();
    }
  }

  const cfgNewer = (a, b) => !b || a.v > b.v || (a.v === b.v && a.by > b.by);

  function applyCfg(d, local) {
    const prev = S.cfg;
    S.cfg = d;
    store.set('cfg:' + S.code, d);
    // 新的一局：清空我的词
    if (S.mine && d.players.includes(S.mine.pid) && S.mine.gid !== d.gid) {
      S.mine = { ...S.mine, gid: d.gid, words: [], seq: S.mine.seq + 1 };
      saveMine();
      publishMine();
    }
    if (!local && prev && S.ready) {
      if (prev.phase === 'play' && d.phase === 'lobby') toast(`${nameOf(d.by, 'TA')} 回到了房间，准备下一局`);
      if (prev.phase === 'lobby' && d.phase === 'play') vibrate(30);
    }
    if (S.ready) ensureSeat();
    render();
  }

  function writeCfg(patch) {
    const d = { ...(S.cfg || {}), ...patch, v: ((S.cfg && S.cfg.v) || 0) + 1, by: S.me.pid };
    applyCfg(d, true);
    publishCfg();
  }

  function saveMine() { if (S.mine) store.set(`st:${S.code}:${S.mine.pid}`, S.mine); }

  // 首次同步结束后决定：建房 / 入座 / 房间已满 / 找不到
  function finishSync() {
    if (S.ready) return;
    S.ready = true;
    S.readyAt = Date.now();
    if (!S.cfg) {
      const cached = store.get('cfg:' + S.code);
      if (cached && S.me && cached.players && cached.players.includes(S.me.pid)) {
        // 服务器丢了数据：用本地缓存恢复房间（repair 会把它补发到服务器）
        S.cfg = cached;
      } else if (S.mode === 'join') {
        S.notFound = true;
      } else {
        createCfg();
      }
    } else {
      ensureSeat();
    }
    render();
  }

  function createCfg() {
    S.notFound = false;
    writeCfg({ phase: 'lobby', gid: rand(8), limit: 60, noRepeat: true, players: [S.me.pid] });
  }

  function ensureSeat() {
    if (!S.cfg || !S.me || S.me.spectator) return;
    if (S.cfg.players.includes(S.me.pid)) { S.seatPick = false; return; }
    if (S.cfg.players.length < 2) { S.seatPick = false; writeCfg({ players: [...S.cfg.players, S.me.pid] }); }
    else S.seatPick = true;
  }

  // ---------- 房间进出 ----------
  const idsOf = code => store.get('ids:' + code, []);
  function rememberId(code, id) {
    const list = idsOf(code).filter(x => x.pid !== id.pid);
    list.unshift({ pid: id.pid, name: id.name, spectator: !!id.spectator });
    store.set('ids:' + code, list.slice(0, 6));
    store.set('last', { code, pid: id.pid, name: id.name });
    try { sessionStorage.setItem('lx:sid:' + code, id.pid); } catch { }
  }
  const sessionPid = code => { try { return sessionStorage.getItem('lx:sid:' + code); } catch { return null; } };

  async function enterRoom(code, name, mode, pid) {
    if (!window.crypto || !crypto.subtle) { toast('请用 https 链接打开本页面'); return; }
    if (typeof mqtt === 'undefined') { toast('脚本加载失败，请刷新页面'); return; }
    if (S.code) leaveRoom(true);
    const gen = ++S.gen;
    Object.assign(S, {
      code, mode, cfg: null, players: {}, presence: {}, ready: false, readyAt: 0,
      notFound: false, seatPick: false, resultFor: null, autoLock: null,
    });
    H.reset();
    // 身份：指定 pid > 同昵称的已知身份 > 新身份（同一设备开两个标签页用不同昵称即可当两个人）
    const list = idsOf(code);
    let id = null;
    if (pid) id = list.find(x => x.pid === pid) || { pid, name: name || '玩家', spectator: false };
    else if (name && mode !== 'create') id = list.find(x => x.name === name && !x.spectator) || null;
    if (!id) id = { pid: rand(10), name: name || '玩家', spectator: false };
    id = { ...id };
    if (name) id.name = name;
    S.me = id;
    rememberId(code, id);
    if (location.hash.slice(1) !== code) history.replaceState(null, '', location.pathname + location.search + '#' + code);
    render();

    try { S.room = await deriveRoom(code); } catch (e) { toast('浏览器不支持加密功能，请换个浏览器'); return; }
    if (gen !== S.gen) return;

    if (!id.spectator) {
      S.mine = store.get(`st:${code}:${id.pid}`) || { pid: id.pid, name: id.name, gid: null, words: [], seq: 0 };
      if (S.mine.name !== id.name) { S.mine.name = id.name; S.mine.seq++; saveMine(); }
    } else S.mine = null;

    connectNet();
    if (mode === 'create') { createCfg(); S.ready = true; S.readyAt = Date.now(); }
    render();
  }

  function leaveRoom(silent) {
    closeNet();
    S.gen++;
    Object.assign(S, { code: null, room: null, me: null, mine: null, cfg: null, players: {}, presence: {}, ready: false, notFound: false, seatPick: false });
    hideOverlay('#result'); hideOverlay('#dialog');
    if (!silent) {
      history.replaceState(null, '', location.pathname + location.search);
      render();
    }
  }

  function takeSeat(pid) {
    const st = S.players[pid];
    const code = S.code;
    const id = { pid, name: (st && st.name) || '玩家', spectator: false };
    rememberId(code, id);
    if (st) store.set(`st:${code}:${pid}`, st);
    leaveRoom(true);
    enterRoom(code, null, 'resume', pid);
  }

  function spectate() {
    S.me.spectator = true;
    rememberId(S.code, S.me);
    S.mine = null;
    S.seatPick = false;
    render();
  }

  // ---------- 推导游戏状态 ----------
  function seatPids() {
    const p = (S.cfg && S.cfg.players) || [];
    if (S.mine && p.includes(S.mine.pid)) return [S.mine.pid, p.find(x => x !== S.mine.pid) || null];
    return [p[0] || null, p[1] || null];
  }
  function stateOf(pid) {
    if (!pid) return null;
    if (S.mine && pid === S.mine.pid) return S.mine;
    return S.players[pid] || null;
  }
  function nameOf(pid, fb = 'TA') { const s = stateOf(pid); return (s && s.name) || fb; }
  function isOnline(pid) {
    if (!pid) return false;
    if (S.mine && pid === S.mine.pid) return anyConnected();
    const p = S.presence[pid];
    return !!p && Object.values(p).some(Boolean);
  }
  function wordsOf(pid) {
    const s = stateOf(pid);
    return s && S.cfg && s.gid === S.cfg.gid ? s.words : [];
  }
  function derive() {
    const [L, R] = seatPids();
    const A = wordsOf(L), B = wordsOf(R);
    const done = Math.min(A.length, B.length);
    let match = -1;
    for (let i = 0; i < done; i++) if (same(A[i], B[i])) { match = i; break; }
    const isPlayer = !!(S.mine && S.cfg && S.cfg.players.includes(S.mine.pid));
    return {
      L, R, A, B, done, match, isPlayer,
      rounds: match >= 0 ? match + 1 : done,
      cur: match >= 0 ? -1 : done,
      lSub: A.length > done,
      rSub: B.length > done,
    };
  }
  function usedWords(d) {
    const set = new Set();
    for (let i = 0; i < d.done; i++) { const a = norm(d.A[i]), b = norm(d.B[i]); if (a) set.add(a); if (b) set.add(b); }
    return set;
  }

  // ---------- 提交 ----------
  function trySubmit(raw, auto = false) {
    const d = derive();
    if (!d.isPlayer || d.cur < 0 || d.lSub || S.cfg.phase !== 'play') return false;
    let w = String(raw || '').trim().replace(/\s+/g, ' ');
    w = Array.from(w).slice(0, MAX_WORD).join('');
    const repeated = S.cfg.noRepeat && norm(w) && usedWords(d).has(norm(w));
    if (!auto) {
      if (!norm(w)) { shake('#wordForm'); toast('先写一个词吧'); return false; }
      if (repeated) { shake('#wordForm'); toast(`「${w}」之前出现过啦，换一个吧`); return false; }
    } else if (!norm(w) || repeated) {
      w = '';
    }
    if (S.mine.gid !== S.cfg.gid) { S.mine.gid = S.cfg.gid; S.mine.words = []; }
    S.mine.words = [...S.mine.words.slice(0, d.done), w];
    S.mine.seq++;
    saveMine();
    publishMine();
    $('#wordInput').value = '';
    if (auto) toast(w ? `时间到，已自动提交「${w}」` : '时间到，这一轮留空了');
    else vibrate(15);
    render();
    return true;
  }

  // ---------- 计时 ----------
  function tick() {
    if (view() !== 'game') return;
    const comp = $('#composer');
    const d = derive();
    const lim = (S.cfg.limit | 0) * 1000;
    const active = lim > 0 && d.isPlayer && d.cur >= 0 && !d.lSub && S.ready;
    comp.classList.toggle('timed', active);
    if (!active) { comp.classList.remove('urgent'); return; }
    const tk = `timer:${S.code}:${S.mine.pid}`;
    let t = store.get(tk);
    if (!t || t.gid !== S.cfg.gid || t.round !== d.cur) {
      t = { gid: S.cfg.gid, round: d.cur, start: Date.now() + ROUND_GRACE };
      store.set(tk, t);
    }
    const remain = Math.max(0, Math.min(lim, lim - (Date.now() - t.start)));
    $('#timerBar').style.transform = `scaleX(${remain / lim})`;
    $('#timerTxt').textContent = Math.ceil(remain / 1000) + 's';
    comp.classList.toggle('urgent', remain <= 5000);
    if (remain <= 0) {
      const lockKey = S.cfg.gid + ':' + d.cur;
      if (S.autoLock !== lockKey) { S.autoLock = lockKey; trySubmit($('#wordInput').value, true); }
    }
  }

  // ---------- 渲染 ----------
  function view() {
    if (!S.code) return 'home';
    if (S.seatPick) return 'seats';
    if (S.cfg && S.cfg.phase === 'play') return 'game';
    return 'lobby';
  }

  function render() {
    const v = view();
    for (const id of ['home', 'lobby', 'seats', 'game']) $('#' + id).classList.toggle('hidden', id !== v);
    if (v === 'home') renderHome();
    else if (v === 'lobby') renderLobby();
    else if (v === 'seats') renderSeats();
    else renderGame();
    if (v !== 'game') hideOverlay('#result');
    updateConn();
  }

  function resumeTarget() {
    const hc = cleanCode(location.hash.slice(1));
    if (validCode(hc)) {
      const id = idsOf(hc).find(x => !x.spectator);
      return id ? { code: hc, pid: id.pid, name: id.name } : null;
    }
    const last = store.get('last');
    return last && typeof last === 'object' && last.code ? last : null;
  }
  function renderHome() {
    const t = resumeTarget();
    const btn = $('#resumeBtn');
    if (t) { btn.classList.remove('hidden'); btn.innerHTML = `回到房间 <b>${esc(t.code)}</b>（${esc(t.name || '我')}）→`; }
    else btn.classList.add('hidden');
  }

  function fillSeat(el, pid, cls, waitingText) {
    el.className = 'seat ' + (pid ? cls : 'empty');
    const av = $('.avatar', el);
    if (!pid) {
      av.innerHTML = '?';
      $('.nm', el).textContent = '等待 TA 加入';
      $('.st', el).innerHTML = `<span class="typing"><i></i><i></i><i></i></span>`;
      return;
    }
    const on = isOnline(pid);
    av.innerHTML = esc(initial(nameOf(pid, '…'))) + `<i class="dot ${on ? 'on' : ''}"></i>`;
    $('.nm', el).textContent = nameOf(pid, '…') + (S.mine && pid === S.mine.pid ? '（我）' : '');
    $('.st', el).textContent = on ? '在线' : (waitingText || '离线');
  }

  function renderLobby() {
    const loading = !S.cfg && !S.notFound;
    $('#lobbyLoading').classList.toggle('hidden', !loading);
    $('#lobbyNotFound').classList.toggle('hidden', !S.notFound || !!S.cfg);
    $('#lobbyMain').classList.toggle('hidden', !S.cfg);
    $('#nfCode').textContent = S.code || '';
    if (!S.cfg) return;

    $('#roomCode').textContent = S.code;
    const [L, R] = seatPids();
    fillSeat($('#seatL'), L, 'a');
    fillSeat($('#seatR'), R, 'b');

    const spectator = !(S.mine && S.cfg.players.includes(S.mine.pid));
    $$('#limitSeg button').forEach(b => {
      b.classList.toggle('on', (+b.dataset.v) === (S.cfg.limit | 0));
      b.disabled = spectator;
    });
    $('#noRepeat').checked = !!S.cfg.noRepeat;
    $('#noRepeat').disabled = spectator;

    const full = S.cfg.players.length >= 2;
    const start = $('#startBtn');
    start.disabled = !full || spectator;
    start.classList.toggle('hidden', spectator);
    let hint = '';
    if (spectator) hint = '你正在观战，等两位玩家开始游戏';
    else if (!full) hint = '把房间号或邀请链接发给 TA，TA 进来后就能开始';
    else if (!isOnline(R)) hint = `${nameOf(R)} 暂时离线，也可以先开始`;
    else hint = '任意一方点「开始游戏」即可，设置双方同步';
    $('#lobbyHint').textContent = hint;
  }

  function renderSeats() {
    const list = $('#seatList');
    const ps = (S.cfg && S.cfg.players) || [];
    list.innerHTML = ps.map((pid, i) => {
      const on = isOnline(pid);
      return `<button class="seat-pick" data-pid="${esc(pid)}">
        <span class="avatar" style="background:var(--${i ? 'b' : 'a'})">${esc(initial(nameOf(pid, '?')))}</span>
        <span class="grow"><b>我是 ${esc(nameOf(pid, '玩家' + (i + 1)))}</b><br><span class="st">${on ? '当前在线（另一台设备）' : '离线'}</span></span>
        <span>›</span></button>`;
    }).join('');
  }

  // 历史记录增量渲染
  const H = {
    gid: null, n: 0, L: null,
    reset() { this.gid = null; this.n = 0; this.L = null; const box = $('#history'); if (box) box.innerHTML = ''; },
  };

  function wordHtml(w) { return w ? esc(w) : '<span class="blank">（超时留空）</span>'; }

  function roundEl(i, a, b, isMatch, animate) {
    const el = document.createElement('div');
    el.className = 'round' + (isMatch ? ' match' : '') + (animate ? ' reveal' : '');
    if (isMatch) {
      el.innerHTML = `<div class="word">${esc(a)}</div><div class="tag">第 ${i + 1} 轮 · 心有灵犀 ✨</div>`;
    } else {
      el.innerHTML = `<div class="word a">${wordHtml(a)}</div><div class="rno">${i + 1}</div><div class="word b">${wordHtml(b)}</div>`;
    }
    return el;
  }

  function renderHistory(d) {
    const box = $('#history');
    if (H.gid !== S.cfg.gid || H.L !== d.L || d.rounds < H.n) {
      box.innerHTML = '';
      H.gid = S.cfg.gid; H.L = d.L; H.n = 0;
    }
    let empty = $('.empty', box);
    if (d.rounds === 0) {
      if (!empty) {
        empty = document.createElement('div');
        empty.className = 'empty';
        empty.innerHTML = `<div class="venn pulse"><i></i><i></i></div><b>第一轮：随便写一个词</b><div>写下你脑子里冒出来的第一个词，<br>看看你们会从哪里出发</div>`;
        box.appendChild(empty);
      }
      return false;
    }
    if (empty) empty.remove();
    const animate = S.ready && Date.now() - S.readyAt > 1200;
    let added = false;
    for (let i = H.n; i < d.rounds; i++) {
      box.appendChild(roundEl(i, d.A[i], d.B[i], i === d.match, animate));
      added = true;
    }
    H.n = d.rounds;
    if (added) {
      requestAnimationFrame(() => box.scrollTo({ top: box.scrollHeight, behavior: animate ? 'smooth' : 'auto' }));
      if (animate) vibrate(d.match >= 0 ? [40, 60, 40] : 20);
    }
    return added && animate;
  }

  function renderGame() {
    const d = derive();
    const live = renderHistory(d);
    const nm = (pid, fb) => nameOf(pid, fb) + (S.mine && pid === S.mine.pid ? '（我）' : '');

    $('#roundTitle').textContent = d.match >= 0 ? `第 ${d.match + 1} 轮 · 达成` : `第 ${d.cur + 1} 轮`;
    $('#gameCode').textContent = S.code;
    $('#nameL').textContent = nm(d.L, '玩家 1');
    $('#nameR').textContent = nm(d.R, '玩家 2');
    $('#dotL').classList.toggle('on', isOnline(d.L));
    $('#dotR').classList.toggle('on', isOnline(d.R));

    const form = $('#wordForm'), waiting = $('#waiting'), doneRow = $('#doneRow'), prompt = $('#prompt'), pst = $('#pStatus');
    const partner = nameOf(d.R);

    if (d.match >= 0) {
      form.classList.add('hidden'); waiting.classList.add('hidden'); doneRow.classList.remove('hidden');
      prompt.innerHTML = `你们在第 ${d.match + 1} 轮写出了同一个词 <b class="a">${esc(d.A[d.match])}</b>`;
      $('#againBtn2').classList.toggle('hidden', !d.isPlayer);
      pst.className = 'pstatus'; pst.textContent = '';
      if (S.resultFor !== S.cfg.gid) {
        S.resultFor = S.cfg.gid;
        if (live) { const gid = S.cfg.gid; setTimeout(() => { if (S.cfg && S.cfg.gid === gid && view() === 'game') showResult(derive(), true); }, 1300); }
        else showResult(d, false);
      }
      return;
    }
    doneRow.classList.add('hidden');

    // 提示语
    if (d.cur === 0) prompt.innerHTML = `第 1 轮 · 随便写一个词`;
    else {
      const pa = d.A[d.cur - 1], pb = d.B[d.cur - 1];
      if (!pa && !pb) prompt.innerHTML = `第 ${d.cur + 1} 轮 · 上一轮都没写，随便写一个吧`;
      else prompt.innerHTML = `第 ${d.cur + 1} 轮 · 由 ${pa ? `<b class="a">${esc(pa)}</b>` : '（空）'} 和 ${pb ? `<b class="b">${esc(pb)}</b>` : '（空）'} 你会想到…`;
    }

    if (!d.isPlayer) {
      form.classList.add('hidden');
      waiting.classList.remove('hidden');
      const s = pid => (wordsOf(pid).length > d.done ? '已写好 ✓' : (isOnline(pid) ? '思考中' : '离线'));
      waiting.innerHTML = `<div class="venn sm pulse"><i></i><i></i></div><div>观战中 · ${esc(nameOf(d.L))} ${s(d.L)}，${esc(nameOf(d.R))} ${s(d.R)}</div>`;
      pst.textContent = '';
      return;
    }

    if (!d.lSub) {
      form.classList.remove('hidden'); waiting.classList.add('hidden');
      const overlayOpen = !$('#result').classList.contains('hidden') || !$('#dialog').classList.contains('hidden');
      if (!isTouch && !overlayOpen && document.activeElement !== $('#wordInput')) $('#wordInput').focus({ preventScroll: true });
    } else {
      form.classList.add('hidden'); waiting.classList.remove('hidden');
      const mine = d.A[d.done];
      waiting.innerHTML = `<div class="venn sm pulse"><i></i><i></i></div><div>${mine ? `你写下了 <b>${esc(mine)}</b>` : '本轮留空'} · 等 ${esc(partner)} 写好就揭晓</div>`;
    }

    // 对方状态
    if (!d.R) { pst.className = 'pstatus'; pst.textContent = '还没有对手加入'; }
    else if (d.rSub) { pst.className = 'pstatus ready'; pst.textContent = `${partner} 已经写好了 ✓`; }
    else if (isOnline(d.R)) { pst.className = 'pstatus'; pst.innerHTML = `<i class="dot on"></i>${esc(partner)} 正在想 <span class="typing"><i></i><i></i><i></i></span>`; }
    else { pst.className = 'pstatus'; pst.innerHTML = `<i class="dot"></i>${esc(partner)} 暂时离线`; }
  }

  function showResult(d, celebrate) {
    const w = d.A[d.match];
    $('#resultKicker').textContent = `第 ${d.match + 1} 轮 · 心有灵犀`;
    $('#resultWord').textContent = w;
    $('#resultSub').textContent = d.match === 0 ? '第一轮就想到一起了，太默契了吧！' : `${nameOf(d.L)} 和 ${nameOf(d.R)} 用 ${d.match + 1} 轮走到了一起`;
    $('#chain').innerHTML = Array.from({ length: d.match + 1 }, (_, i) => i === d.match
      ? `<div class="chain-row m"><span class="i">${i + 1}</span><span class="x" style="grid-column:span 2">${esc(d.A[i])} ✨</span></div>`
      : `<div class="chain-row"><span class="i">${i + 1}</span><span class="x a">${d.A[i] ? esc(d.A[i]) : '—'}</span><span class="x b">${d.B[i] ? esc(d.B[i]) : '—'}</span></div>`
    ).join('');
    $('#againBtn').classList.toggle('hidden', !d.isPlayer);
    const venn = $('#resultVenn');
    venn.classList.remove('merged'); void venn.offsetWidth; venn.classList.add('merged');
    showOverlay('#result');
    if (celebrate) setTimeout(confetti, 250);
  }

  function resultText() {
    const d = derive();
    if (d.match < 0) return '';
    const lines = [`我们在第 ${d.match + 1} 轮心有灵犀 ✨「${d.A[d.match]}」`];
    for (let i = 0; i < d.match; i++) lines.push(`${i + 1}. ${d.A[i] || '—'} / ${d.B[i] || '—'}`);
    lines.push(`${d.match + 1}. ${d.A[d.match]} = ${d.B[d.match]}`);
    lines.push('', '一起来玩：' + location.origin + location.pathname);
    return lines.join('\n');
  }

  function updateConn() {
    const n = S.net ? S.net.clients.filter(c => c && c.connected).length : 0;
    const total = S.net ? S.net.clients.filter(Boolean).length : 0;
    for (const el of $$('[data-conn]')) {
      el.className = 'conn' + (n > 0 ? ' ok' : (S.net ? ' ing' : ' bad'));
      el.title = n > 0 ? `已连接 ${n}/${total} 条线路` : '连接中…';
    }
    const t = $('#connTxt');
    if (t) t.textContent = n > 0 ? (total > 1 ? `已连接 ${n}/${total}` : '已连接') : '连接中…';
  }

  // ---------- 小组件 ----------
  let toastTimer;
  function toast(msg) {
    const el = $('#toast');
    el.textContent = msg;
    el.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => el.classList.remove('show'), 2400);
  }
  function shake(sel) { const el = $(sel); el.classList.remove('shake'); void el.offsetWidth; el.classList.add('shake'); }
  function vibrate(p) { try { navigator.vibrate && navigator.vibrate(p); } catch { } }
  function showOverlay(sel) { $(sel).classList.remove('hidden'); }
  function hideOverlay(sel) { $(sel).classList.add('hidden'); }

  function confirmDlg(title, text, okText = '确定') {
    return new Promise(resolve => {
      $('#dlgTitle').textContent = title;
      $('#dlgText').textContent = text;
      $('#dlgOk').textContent = okText;
      showOverlay('#dialog');
      const done = v => { hideOverlay('#dialog'); $('#dlgOk').onclick = $('#dlgCancel').onclick = $('#dialog').onclick = null; resolve(v); };
      $('#dlgOk').onclick = () => done(true);
      $('#dlgCancel').onclick = () => done(false);
      $('#dialog').onclick = e => { if (e.target.id === 'dialog') done(false); };
    });
  }

  async function copyText(text) {
    try { await navigator.clipboard.writeText(text); return true; }
    catch {
      const ta = document.createElement('textarea');
      ta.value = text; ta.setAttribute('readonly', ''); ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      let ok = false; try { ok = document.execCommand('copy'); } catch { }
      ta.remove(); return ok;
    }
  }

  function inviteLink() { return `${location.origin}${location.pathname}${location.search}#${S.code}`; }

  // 彩带
  function confetti() {
    const cv = $('#confetti'), ctx = cv.getContext('2d');
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const W = cv.width = innerWidth * dpr, Hh = cv.height = innerHeight * dpr;
    const css = getComputedStyle(document.documentElement);
    const colors = ['--a', '--b', '--mix'].map(v => css.getPropertyValue(v).trim()).concat(['#F5C451', '#FFFFFF']);
    const parts = Array.from({ length: 140 }, () => ({
      x: W / 2 + (Math.random() - .5) * W * .3, y: Hh * .45,
      vx: (Math.random() - .5) * 22 * dpr, vy: (-Math.random() * 20 - 8) * dpr,
      s: (Math.random() * 6 + 5) * dpr, r: Math.random() * 6, vr: (Math.random() - .5) * .3,
      c: colors[(Math.random() * colors.length) | 0], heart: Math.random() < .25,
    }));
    const t0 = performance.now();
    (function frame(t) {
      const el = t - t0;
      ctx.clearRect(0, 0, W, Hh);
      for (const p of parts) {
        p.vy += .55 * dpr; p.vx *= .985; p.vy *= .985;
        p.x += p.vx; p.y += p.vy; p.r += p.vr;
        ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.r);
        ctx.globalAlpha = Math.max(0, 1 - el / 3200);
        ctx.fillStyle = p.c;
        if (p.heart) {
          const s = p.s * .9;
          ctx.beginPath(); ctx.moveTo(0, s * .3);
          ctx.bezierCurveTo(-s, -s * .4, -s * .4, -s, 0, -s * .35);
          ctx.bezierCurveTo(s * .4, -s, s, -s * .4, 0, s * .3);
          ctx.fill();
        } else ctx.fillRect(-p.s / 2, -p.s / 4, p.s, p.s / 2);
        ctx.restore();
      }
      if (el < 3300) requestAnimationFrame(frame); else ctx.clearRect(0, 0, W, Hh);
    })(t0);
  }

  // 视口高度（处理手机键盘）
  function fitViewport() {
    const h = window.visualViewport ? window.visualViewport.height : innerHeight;
    document.documentElement.style.setProperty('--app-h', h + 'px');
    if (view() === 'game') window.scrollTo(0, 0);
  }

  // ---------- 事件 ----------
  function bind() {
    const nameInput = $('#nameInput'), codeInput = $('#codeInput');
    nameInput.value = store.get('name', '');
    const getName = () => {
      const n = nameInput.value.trim().slice(0, 12);
      if (!n) { shake('#nameInput'); nameInput.focus(); toast('先给自己起个昵称吧'); return null; }
      store.set('name', n);
      return n;
    };

    $('#createBtn').onclick = () => { const n = getName(); if (n) enterRoom(newCode(), n, 'create'); };
    const join = () => {
      const c = cleanCode(codeInput.value);
      if (!validCode(c)) { shake('.join-row'); toast('房间号是 6 位字母或数字'); codeInput.focus(); return; }
      const n = getName(); if (n) enterRoom(c, n, 'join');
    };
    $('#joinBtn').onclick = join;
    codeInput.addEventListener('input', () => { codeInput.value = cleanCode(codeInput.value).slice(0, CODE_LEN); });
    codeInput.addEventListener('keydown', e => { if (e.key === 'Enter') join(); });
    nameInput.addEventListener('keydown', e => { if (e.key === 'Enter') (cleanCode(codeInput.value).length === CODE_LEN ? join() : $('#createBtn').click()); });
    $('#resumeBtn').onclick = () => { const t = resumeTarget(); if (t) enterRoom(t.code, null, 'resume', t.pid); };

    document.addEventListener('click', async e => {
      const a = e.target.closest('[data-act="leave"]');
      if (!a) return;
      if (view() === 'game' && derive().match < 0 && derive().isPlayer) {
        if (!await confirmDlg('离开房间？', '游戏进度会保留，之后用同一个链接回来就能继续。', '离开')) return;
      }
      leaveRoom();
    });

    $('#roomCode').onclick = async () => { if (await copyText(S.code)) toast('房间号已复制'); };
    $('#shareBtn').onclick = async () => {
      const url = inviteLink();
      const text = `来和我玩「心有灵犀」吧！房间号 ${S.code}`;
      if (navigator.share && isTouch) {
        try { await navigator.share({ title: '心有灵犀', text, url }); return; } catch (e) { if (e && e.name === 'AbortError') return; }
      }
      if (await copyText(`${text}\n${url}`)) toast('邀请链接已复制，发给 TA 吧');
      else toast(url);
    };
    $('#limitSeg').onclick = e => {
      const b = e.target.closest('button'); if (!b || b.disabled) return;
      writeCfg({ limit: +b.dataset.v });
    };
    $('#noRepeat').onchange = e => writeCfg({ noRepeat: e.target.checked });
    $('#startBtn').onclick = () => {
      if (!S.cfg || S.cfg.players.length < 2) return;
      writeCfg({ phase: 'play', gid: rand(8), startedAt: Date.now() });
    };
    $('#nfCreate').onclick = () => { S.mode = 'create'; createCfg(); };

    $('#seatList').onclick = e => { const b = e.target.closest('[data-pid]'); if (b) takeSeat(b.dataset.pid); };
    $('#spectateBtn').onclick = spectate;

    $('#wordForm').addEventListener('submit', e => { e.preventDefault(); trySubmit($('#wordInput').value); });

    const again = () => { hideOverlay('#result'); writeCfg({ phase: 'lobby' }); };
    $('#againBtn').onclick = again;
    $('#againBtn2').onclick = again;
    $('#closeResult').onclick = () => hideOverlay('#result');
    $('#result').onclick = e => { if (e.target.id === 'result') hideOverlay('#result'); };
    $('#showResultBtn').onclick = () => showResult(derive(), false);
    $('#copyBtn').onclick = async () => { if (await copyText(resultText())) toast('战绩已复制'); };

    $('#menuBtn').onclick = async () => {
      const d = derive();
      if (!d.isPlayer) { toast('观战中'); return; }
      if (await confirmDlg('结束这一局？', '双方都会回到房间，可以重新设置限时后再开一局。', '结束本局')) writeCfg({ phase: 'lobby' });
    };

    // 回到前台时立即重连
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState !== 'visible' || !S.net) return;
      for (const c of S.net.clients.filter(Boolean)) if (!c.connected && !c.reconnecting) { try { c.reconnect(); } catch { } }
      render();
    });
    window.addEventListener('pagehide', () => {
      if (!S.net || !S.me || S.me.spectator) return;
      for (const c of S.net.clients.filter(Boolean)) if (c.connected) { try { c.publish(`${S.net.base}/on/${S.me.pid}`, '0', { qos: 0, retain: true }); } catch { } }
    });
    window.addEventListener('pageshow', e => {
      if (!e.persisted || !S.net) return;
      S.net.clients.forEach((c, i) => {
        if (!c) return;
        if (!c.connected) { try { c.reconnect(); } catch { } }
        else if (S.me && !S.me.spectator) { rawPub('on/' + S.me.pid, '1', true, 0, c); repair(i); }
      });
    });
    window.addEventListener('hashchange', () => {
      const c = cleanCode(location.hash.slice(1));
      if (validCode(c) && c !== S.code) boot();
    });
    if (window.visualViewport) visualViewport.addEventListener('resize', fitViewport);
    window.addEventListener('resize', fitViewport);
  }

  function boot() {
    const c = cleanCode(location.hash.slice(1));
    if (validCode(c)) {
      const sid = sessionPid(c);
      if (sid && idsOf(c).some(x => x.pid === sid)) { enterRoom(c, null, 'resume', sid); return; }
      if (S.code) leaveRoom(true);
      $('#codeInput').value = c;
      render();
      const nameInput = $('#nameInput');
      $('#joinBtn').classList.add('primary');
      $('#createBtn').classList.remove('primary');
      if (!nameInput.value) setTimeout(() => nameInput.focus(), 300);
      toast(`输入昵称，点「加入」进入房间 ${c}`);
      return;
    }
    render();
  }

  // 每秒刷新在线状态 / 计时
  setInterval(tick, 200);
  setInterval(() => { if (S.code && view() !== 'game') render(); }, 3000);

  fitViewport();
  bind();
  boot();

  // 便于调试
  window.__lingxi = { S, derive };
})();
