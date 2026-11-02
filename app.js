/* 心有灵犀 · Kindred — 双人实时联想游戏
 *
 * 同步模型（无需自建后端）：
 *   每个房间对应 MQTT 主题  <namespace>/<hash>/...
 *     cfg        房间配置（玩家、限时、阶段、本局 id），任何玩家可写，版本号 + pid 决胜
 *     p/<pid>    每位玩家自己的状态（昵称、本局写过的词），只有本人写，seq 递增
 *     on/<pid>   在线状态 "1"/"0"（遗嘱消息自动置 0）
 *   cfg 与 p/* 都是 retained 消息，刷新/重连后可立即恢复；内容用 AES-GCM 加密。
 *   同时连接多个服务器，消息按版本去重，任意一条线路通即可。
 *   界面语言只影响本机显示，每位玩家各自选择。
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
  const MAX_WORD = 24;         // 字（字素）数上限
  const MAX_NAME = 14;
  const ROUND_GRACE = 1100;    // 揭晓动画后再开始计时（毫秒）
  const SYNC_WAIT = 1600;      // 订阅后等待保留消息的时间
  const FONT_PROBES = ['的', '概', '宴', '篘', '瑾', '疸', '國']; // 每个中文字体分片里的一个字，用于预加载

  // ---------- 文案 ----------
  const I18N = {
    zh: {
      docTitle: '心有灵犀 · 双人联想游戏',
      appTitle: '心有灵犀',
      tagline: '两个人，一词一词地靠近，<br>直到写下同一个词。',
      nameLabel: '你的名字',
      namePh: '比如：小鱼',
      create: '创建房间',
      haveCode: '已有房间号？',
      codePh: '6 位房间号',
      join: '加入',
      resume: (code, name) => `回到房间 <b>${code}</b> · ${name}`,
      howTitle: '怎么玩',
      rules: [
        '两人各自写下一个词，写好后同时揭晓。',
        '下一轮，看着上一轮的两个词，各自再联想一个。',
        '一轮一轮地靠近，直到你们写下同一个词。',
        '出现过的词不能再用，这条规则也可以在房间里关掉。',
      ],
      back: '返回',
      more: '更多',
      room: '房间',
      roomCode: '房间号',
      tapCopy: '轻点复制',
      invite: '发送邀请',
      waitingSeat: '等 TA 加入',
      online: '在线',
      away: '暂时离开',
      me: '我',
      partner: 'TA',
      perRound: '每轮限时',
      s30: '30 秒', s60: '60 秒', noLimit: '不限时',
      noRepeat: '不能重复用词',
      begin: '开始',
      hintAlone: '把房间号或链接发给 TA，TA 进来就能开始',
      hintAway: n => `${n} 暂时离开了，也可以先开始`,
      hintReady: '设置会同步给双方，谁都可以点开始',
      hintWatch: '你在观战，等两位玩家开始',
      loading: '正在进入房间…',
      nfTitle: '没有找到这个房间',
      nfText: c => `房间号 ${c} 可能写错了，或者已经失效。`,
      nfCreate: '用这个号码开房间',
      fullTitle: '这个房间已经坐满了',
      fullText: '如果你是其中一位（比如换了手机），点自己的名字继续。',
      iAm: n => `我是 ${n}`,
      seatOnline: '正在另一台设备上',
      watch: '只是看看',
      playerN: n => `玩家 ${n}`,
      round: n => `第 ${n} 轮`,
      syncTitle: '心有灵犀',
      emptyTitle: '第一个词，随心就好',
      emptyText: '写下脑海里冒出的第一个词，<br>看看你们从哪里出发。',
      promptFirst: '随心写一个词',
      promptBridge: (a, b) => `在「${a}」与「${b}」之间，你想到了什么？`,
      promptOne: a => `从「${a}」出发，你想到了什么？`,
      promptBlank: '上一轮都留白了，重新随心写一个',
      wordPh: '写下你的词',
      send: '提交',
      blank: '留白',
      sealedText: n => `等 ${n} 写好，一起揭晓`,
      partnerThinking: n => `${n} 正在想`,
      partnerReady: n => `${n} 写好了`,
      partnerAway: n => `${n} 暂时离开`,
      noPartner: '还没有人加入',
      watchLine: (a, sa, b, sb) => `观战中 · ${a} ${sa} · ${b} ${sb}`,
      stThinking: '思考中', stReady: '已写好', stAway: '离开',
      doneLine: w => `你们都写下了「${w}」`,
      matchTag: n => `第 ${n} 轮 · 心有灵犀`,
      connected: '已连接',
      connectedN: (n, t) => `已连接 ${n}/${t}`,
      connecting: '连接中…',
      resultKicker: n => `第 ${n} 轮 · 心有灵犀`,
      resultSub: (a, b, n) => `${a} 和 ${b} 走了 ${n} 轮，写下了同一个词`,
      resultFirst: '第一轮就想到了一起，这份默契太难得了',
      again: '再来一局',
      copyRecap: '复制战绩',
      review: '回看每一轮',
      seeResult: '查看结果',
      codeCopied: '房间号已复制',
      inviteCopied: '邀请链接已复制，发给 TA 吧',
      inviteText: c => `来和我玩「心有灵犀」吧，房间号 ${c}`,
      recapHead: (n, w) => `我们在第 ${n} 轮心有灵犀：「${w}」`,
      recapFoot: '一起来玩：',
      recapCopied: '战绩已复制',
      tJoined: n => `${n} 来了`,
      tEnded: n => `${n} 结束了这一局`,
      tWriteFirst: '先写一个词吧',
      tRepeat: w => `「${w}」已经出现过了，换一个吧`,
      tTimeoutSent: w => `时间到，已替你交上「${w}」`,
      tTimeoutBlank: '时间到，这一轮留白',
      tNeedName: '先给自己起个名字吧',
      tBadCode: '房间号是 6 位字母或数字',
      tHttps: '请用 https 链接打开',
      tNoCrypto: '这个浏览器不支持加密，换一个试试',
      tScript: '脚本加载失败，请刷新页面',
      tOffline: '暂时连不上服务器，正在重试…',
      tInvite: c => `填个名字，就能加入房间 ${c}`,
      tWatching: '观战中',
      leaveTitle: '离开房间？',
      leaveText: '进度会保留，用同一个链接回来就能继续。',
      leave: '离开', stay: '留下',
      endTitle: '结束这一局？',
      endText: '你们会一起回到房间，可以重新设置后再开一局。',
      endGame: '结束本局', cancel: '取消',
      menuInvite: '复制邀请链接', menuEnd: '结束本局',
    },
    en: {
      docTitle: 'Kindred · A word game for two',
      appTitle: 'Kindred',
      tagline: 'Two minds, wandering word by word,<br>until they meet.',
      nameLabel: 'Your name',
      namePh: 'e.g. Juniper',
      create: 'Create a room',
      haveCode: 'Have a room code?',
      codePh: '6-character code',
      join: 'Join',
      resume: (code, name) => `Back to room <b>${code}</b> · ${name}`,
      howTitle: 'How to play',
      rules: [
        'You each write a word in secret. Both are revealed at the same moment.',
        'Next round, look at those two words and each write a new one that bridges them.',
        'Round by round, drift closer — until you both write the very same word.',
        'A word can only be played once. You can turn this off in the room.',
      ],
      back: 'Back',
      more: 'More',
      room: 'Room',
      roomCode: 'Room code',
      tapCopy: 'Tap to copy',
      invite: 'Send invite',
      waitingSeat: 'Waiting for them',
      online: 'Online',
      away: 'Away',
      me: 'You',
      partner: 'your partner',
      perRound: 'Time per round',
      s30: '30s', s60: '60s', noLimit: 'No limit',
      noRepeat: 'No repeated words',
      begin: 'Begin',
      hintAlone: 'Share the code or link — you can begin once they arrive.',
      hintAway: n => `${n} has stepped away. You can still begin.`,
      hintReady: 'Settings are shared. Either of you can begin.',
      hintWatch: 'You’re watching. The game starts when both players are ready.',
      loading: 'Entering the room…',
      nfTitle: 'Room not found',
      nfText: c => `The code ${c} may be mistyped, or the room has expired.`,
      nfCreate: 'Open it as a new room',
      fullTitle: 'This room is full',
      fullText: 'If you’re one of the players on a new device, tap your name to continue.',
      iAm: n => `I’m ${n}`,
      seatOnline: 'Active on another device',
      watch: 'Just watch',
      playerN: n => `Player ${n}`,
      round: n => `Round ${n}`,
      syncTitle: 'In sync',
      emptyTitle: 'Start anywhere',
      emptyText: 'Write the first word that comes to mind,<br>and see where the two of you begin.',
      promptFirst: 'Any word to begin',
      promptBridge: (a, b) => `Somewhere between ${a} and ${b}…`,
      promptOne: a => `Starting from ${a}…`,
      promptBlank: 'Both blank last round — start fresh',
      wordPh: 'Your word',
      send: 'Send',
      blank: 'blank',
      sealedText: n => `Revealed when ${n} is ready`,
      partnerThinking: n => `${n} is thinking`,
      partnerReady: n => `${n} is ready`,
      partnerAway: n => `${n} stepped away`,
      noPartner: 'No one has joined yet',
      watchLine: (a, sa, b, sb) => `Watching · ${a} ${sa} · ${b} ${sb}`,
      stThinking: 'thinking', stReady: 'ready', stAway: 'away',
      doneLine: w => `You both wrote ${w}`,
      matchTag: n => `Round ${n} · In sync`,
      connected: 'Connected',
      connectedN: (n, t) => `Connected ${n}/${t}`,
      connecting: 'Connecting…',
      resultKicker: n => `Round ${n} · In sync`,
      resultSub: (a, b, n) => `${a} and ${b} found the same word in ${n} rounds.`,
      resultFirst: 'The same word on the very first try. Remarkable.',
      again: 'Play again',
      copyRecap: 'Copy recap',
      review: 'See every round',
      seeResult: 'See result',
      codeCopied: 'Room code copied',
      inviteCopied: 'Invite link copied',
      inviteText: c => `Come play Kindred with me — room ${c}`,
      recapHead: (n, w) => `We found the same word in round ${n}: “${w}”`,
      recapFoot: 'Play here: ',
      recapCopied: 'Recap copied',
      tJoined: n => `${n} is here`,
      tEnded: n => `${n} ended the game`,
      tWriteFirst: 'Write a word first',
      tRepeat: w => `“${w}” has already been played — try another`,
      tTimeoutSent: w => `Time’s up — sent “${w}” for you`,
      tTimeoutBlank: 'Time’s up — this round stays blank',
      tNeedName: 'Give yourself a name first',
      tBadCode: 'Room codes are 6 letters or numbers',
      tHttps: 'Please open this page over https',
      tNoCrypto: 'This browser can’t encrypt — try another one',
      tScript: 'Something failed to load — please refresh',
      tOffline: 'Can’t reach the server yet — retrying…',
      tInvite: c => `Add your name to join room ${c}`,
      tWatching: 'Watching',
      leaveTitle: 'Leave the room?',
      leaveText: 'Your progress is saved. Come back with the same link to pick up where you left off.',
      leave: 'Leave', stay: 'Stay',
      endTitle: 'End this game?',
      endText: 'You’ll both return to the room and can set up a new one.',
      endGame: 'End game', cancel: 'Cancel',
      menuInvite: 'Copy invite link', menuEnd: 'End this game',
    },
  };

  // ---------- 工具 ----------
  const $ = (s, el = document) => el.querySelector(s);
  const $$ = (s, el = document) => Array.from(el.querySelectorAll(s));
  const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const rand = (n = 10) => Array.from(crypto.getRandomValues(new Uint8Array(n)), x => (x % 36).toString(36)).join('');
  const newCode = () => Array.from(crypto.getRandomValues(new Uint8Array(CODE_LEN)), x => CODE_ALPHABET[x % CODE_ALPHABET.length]).join('');
  const cleanCode = s => String(s || '').toUpperCase().replace(/[^0-9A-Z]/g, '');
  const validCode = c => c.length === CODE_LEN && [...c].every(ch => CODE_ALPHABET.includes(ch));
  const graphemes = s => {
    if (window.Intl && Intl.Segmenter) return Array.from(new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(s), x => x.segment);
    return Array.from(s);
  };
  const clip = (s, n) => graphemes(String(s || '').trim().replace(/\s+/g, ' ')).slice(0, n).join('').trim();
  const initial = name => (graphemes(String(name || '?').trim())[0] || '?').toUpperCase();
  const isTouch = matchMedia('(pointer: coarse)').matches;
  const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
  const svgCheck = '<svg class="chk" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7.5" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const icon = id => `<svg class="ic" aria-hidden="true"><use href="#${id}"/></svg>`;

  // 判断两个词是否"相同"：忽略大小写、空格、标点、全半角、西文重音、英文冠词与简单复数
  function canon(s) {
    let x = String(s || '').normalize('NFKC').toLowerCase().trim();
    x = x.normalize('NFD').replace(/[̀-ͯ]/g, '').normalize('NFC');
    x = x.replace(/^(the|a|an)\s+/, '');
    return x.replace(/[\s\p{P}\p{S}]/gu, '');
  }
  function same(a, b) {
    const x = canon(a), y = canon(b);
    if (!x || !y) return false;
    if (x === y) return true;
    if (/^[a-z]+$/.test(x) && /^[a-z]+$/.test(y)) {
      const [s, l] = x.length < y.length ? [x, y] : [y, x];
      if (s.length >= 3 && (l === s + 's' || l === s + 'es' || (s.endsWith('y') && l === s.slice(0, -1) + 'ies'))) return true;
    }
    return false;
  }

  const store = {
    get(k, d = null) { try { const v = localStorage.getItem('lx:' + k); return v == null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem('lx:' + k, JSON.stringify(v)); } catch { } },
  };

  // ---------- 语言 ----------
  let LANG = document.documentElement.getAttribute('data-lang') === 'en' ? 'en' : 'zh';
  function t(key, ...args) {
    const v = I18N[LANG][key] ?? I18N.zh[key];
    return typeof v === 'function' ? v(...args) : v;
  }
  function applyStatic() {
    document.title = t('docTitle');
    for (const el of $$('[data-i18n]')) el.textContent = t(el.dataset.i18n);
    for (const el of $$('[data-i18n-html]')) el.innerHTML = t(el.dataset.i18nHtml);
    for (const el of $$('[data-i18n-ph]')) el.placeholder = t(el.dataset.i18nPh);
    for (const el of $$('[data-i18n-aria]')) el.setAttribute('aria-label', t(el.dataset.i18nAria));
    $('#rulesList').innerHTML = t('rules').map(r => `<li><span>${esc(r)}</span></li>`).join('');
  }
  function setLang(l, animate = true) {
    if (l !== 'zh' && l !== 'en') return;
    if (l === LANG && animate) return;
    LANG = l;
    store.set('lang', l);
    const html = document.documentElement;
    html.lang = l === 'zh' ? 'zh-CN' : 'en';
    html.setAttribute('data-lang', l);
    applyStatic();
    H.reset();
    lastPrompt = null;
    render();
    if (S.resultOpen) fillResult(derive());
    if (animate && !reduced) {
      html.classList.remove('lang-swap'); void html.offsetWidth; html.classList.add('lang-swap');
      clearTimeout(setLang.tm); setLang.tm = setTimeout(() => html.classList.remove('lang-swap'), 500);
    }
  }

  // ---------- 状态 ----------
  const S = {
    gen: 0, code: null, room: null, me: null, mine: null, cfg: null,
    players: {}, presence: {}, net: null, mode: null,
    ready: false, readyAt: 0, notFound: false, seatPick: false,
    resultFor: null, resultOpen: false, autoLock: null, seen: {},
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
        if (ev !== 'reconnect') for (const p in S.presence) delete S.presence[p][i];
        updateConn();
        render();
      });
      c.on('error', e => console.warn('[mqtt]', url, e && e.message));
      net.clients[i] = c;
    });
    setTimeout(() => { if (gen === S.gen && !anyConnected()) toast(t('tOffline')); }, 9000);
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
        toast(t('tJoined', d.name));
      }
      render();
    }
  }

  const cfgNewer = (a, b) => !b || a.v > b.v || (a.v === b.v && a.by > b.by);

  function applyCfg(d, local) {
    const prev = S.cfg;
    S.cfg = d;
    store.set('cfg:' + S.code, d);
    if (S.mine && d.players.includes(S.mine.pid) && S.mine.gid !== d.gid) {
      S.mine = { ...S.mine, gid: d.gid, words: [], seq: S.mine.seq + 1 };
      saveMine();
      publishMine();
    }
    if (!local && prev && S.ready) {
      if (prev.phase === 'play' && d.phase === 'lobby' && d.by !== (S.me && S.me.pid)) toast(t('tEnded', nameOf(d.by)));
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

  function finishSync() {
    if (S.ready) return;
    S.ready = true;
    S.readyAt = Date.now();
    if (!S.cfg) {
      const cached = store.get('cfg:' + S.code);
      if (cached && S.me && cached.players && cached.players.includes(S.me.pid)) S.cfg = cached;
      else if (S.mode === 'join') S.notFound = true;
      else createCfg();
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
    if (!window.crypto || !crypto.subtle) { toast(t('tHttps')); return; }
    if (typeof mqtt === 'undefined') { toast(t('tScript')); return; }
    if (S.code) leaveRoom(true);
    const gen = ++S.gen;
    Object.assign(S, {
      code, mode, cfg: null, players: {}, presence: {}, ready: false, readyAt: 0,
      notFound: false, seatPick: false, resultFor: null, autoLock: null, seen: {},
    });
    H.reset();
    const list = idsOf(code);
    let id = null;
    if (pid) id = list.find(x => x.pid === pid) || { pid, name: name || t('playerN', 1), spectator: false };
    else if (name && mode !== 'create') id = list.find(x => x.name === name && !x.spectator) || null;
    if (!id) id = { pid: rand(10), name: name || t('playerN', 1), spectator: false };
    id = { ...id };
    if (name) id.name = name;
    S.me = id;
    rememberId(code, id);
    if (location.hash.slice(1) !== code) history.replaceState(null, '', location.pathname + location.search + '#' + code);
    render();

    try { S.room = await deriveRoom(code); } catch (e) { toast(t('tNoCrypto')); return; }
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
    closeOverlay('#result', true); closeOverlay('#dialog', true); closeOverlay('#menu', true);
    if (!silent) {
      history.replaceState(null, '', location.pathname + location.search);
      $('#codeInput').value = '';
      $('#createBtn').classList.add('primary');
      $('#joinBtn').classList.remove('primary');
      render();
    }
  }

  function takeSeat(pid) {
    const st = S.players[pid];
    const code = S.code;
    const id = { pid, name: (st && st.name) || t('playerN', 1), spectator: false };
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
  function nameOf(pid, fb) { const s = stateOf(pid); return (s && s.name) || fb || t('partner'); }
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
  function isUsed(w, d) {
    for (let i = 0; i < d.done; i++) if (same(w, d.A[i]) || same(w, d.B[i])) return true;
    return false;
  }

  // ---------- 提交 ----------
  function trySubmit(raw, auto = false) {
    const d = derive();
    if (!d.isPlayer || d.cur < 0 || d.lSub || S.cfg.phase !== 'play') return false;
    let w = clip(raw, MAX_WORD);
    const repeated = S.cfg.noRepeat && canon(w) && isUsed(w, d);
    if (!auto) {
      if (!canon(w)) { shake('#wordForm'); toast(t('tWriteFirst')); return false; }
      if (repeated) { shake('#wordForm'); toast(t('tRepeat', w)); return false; }
    } else if (!canon(w) || repeated) {
      w = '';
    }
    if (S.mine.gid !== S.cfg.gid) { S.mine.gid = S.cfg.gid; S.mine.words = []; }
    S.mine.words = [...S.mine.words.slice(0, d.done), w];
    S.mine.seq++;
    saveMine();
    publishMine();
    const input = $('#wordInput');
    input.value = '';
    if (auto) toast(w ? t('tTimeoutSent', w) : t('tTimeoutBlank'));
    else vibrate(12);
    if (isTouch) input.blur();
    render();
    return true;
  }

  // ---------- 计时 ----------
  let lastSec = null;
  function tick() {
    if (view() !== 'game') return;
    const comp = $('#composer');
    const d = derive();
    const lim = (S.cfg.limit | 0) * 1000;
    const active = lim > 0 && d.isPlayer && d.cur >= 0 && !d.lSub && S.ready;
    comp.classList.toggle('timed', active);
    if (!active) { comp.classList.remove('warn', 'urgent'); lastSec = null; return; }
    const tk = `timer:${S.code}:${S.mine.pid}`;
    let tm = store.get(tk);
    if (!tm || tm.gid !== S.cfg.gid || tm.round !== d.cur) {
      tm = { gid: S.cfg.gid, round: d.cur, start: Date.now() + ROUND_GRACE };
      store.set(tk, tm);
    }
    const remain = Math.max(0, Math.min(lim, lim - (Date.now() - tm.start)));
    $('#timerBar').style.transform = `scaleX(${remain / lim})`;
    const sec = Math.ceil(remain / 1000);
    const pill = $('#timerPill');
    if (sec !== lastSec) {
      pill.textContent = sec + 's';
      if (lastSec != null && sec <= 10) { pill.classList.remove('tick'); void pill.offsetWidth; pill.classList.add('tick'); }
      if (sec === 5 && lastSec === 6) vibrate(10);
      lastSec = sec;
    }
    comp.classList.toggle('warn', remain <= 10000 && remain > 5000);
    comp.classList.toggle('urgent', remain <= 5000);
    if (remain <= 0) {
      const lockKey = S.cfg.gid + ':' + d.cur;
      if (S.autoLock !== lockKey) { S.autoLock = lockKey; trySubmit($('#wordInput').value, true); }
    }
  }

  // ---------- 渲染 ----------
  let lastView = null;
  function view() {
    if (!S.code) return 'home';
    if (S.seatPick) return 'seats';
    if (S.cfg && S.cfg.phase === 'play') return 'game';
    return 'lobby';
  }

  function enterAnim(el) {
    if (reduced || !el) return;
    Array.from(el.children).forEach((c, i) => c.style.setProperty('--i', i));
    el.classList.remove('enter'); void el.offsetWidth; el.classList.add('enter');
    clearTimeout(el._enterT);
    el._enterT = setTimeout(() => el.classList.remove('enter'), 1600);
  }

  function render() {
    const v = view();
    for (const id of ['home', 'lobby', 'seats', 'game']) $('#' + id).classList.toggle('hidden', id !== v);
    if (v !== lastView) {
      lastView = v;
      enterAnim($('#' + v));
      if (v === 'game') preloadFonts(4);
    }
    if (v === 'home') renderHome();
    else if (v === 'lobby') renderLobby();
    else if (v === 'seats') renderSeats();
    else renderGame();
    if (v !== 'game') { closeOverlay('#result', true); closeOverlay('#menu', true); }
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
    const r = resumeTarget();
    const btn = $('#resumeBtn');
    if (r) { btn.classList.remove('hidden'); $('.resume-text', btn).innerHTML = t('resume', esc(r.code), esc(r.name || t('me'))); }
    else btn.classList.add('hidden');
  }

  let lobbyMainShown = false, lastSeatR = undefined;
  function fillSeat(el, pid, cls) {
    const nm = $('.nm', el), av = $('.av-txt', el), st = $('.st', el), dot = $('.dot', el);
    if (!pid) {
      el.className = 'seat empty';
      av.textContent = '';
      nm.innerHTML = `<span class="nm-txt">${esc(t('waitingSeat'))}</span>`;
      st.innerHTML = '<span class="typing"><i></i><i></i><i></i></span>';
      return;
    }
    const on = isOnline(pid);
    const isMe = S.mine && pid === S.mine.pid;
    const arrive = el.classList.contains('arrive');
    el.className = 'seat ' + cls + (arrive ? ' arrive' : '');
    av.textContent = initial(nameOf(pid, '…'));
    dot.classList.toggle('on', on);
    nm.innerHTML = `<span class="nm-txt">${esc(nameOf(pid, '…'))}</span>${isMe ? `<span class="me-tag">${esc(t('me'))}</span>` : ''}`;
    st.textContent = on ? t('online') : t('away');
  }

  function renderLobby() {
    const loading = !S.cfg && !S.notFound;
    $('#lobbyLoading').classList.toggle('hidden', !loading);
    $('#lobbyNotFound').classList.toggle('hidden', !S.notFound || !!S.cfg);
    const main = $('#lobbyMain');
    main.classList.toggle('hidden', !S.cfg);
    $('#nfText').textContent = t('nfText', S.code || '');
    if (!S.cfg) { lobbyMainShown = false; lastSeatR = undefined; return; }
    if (!lobbyMainShown) { lobbyMainShown = true; enterAnim(main); }

    $('#roomCode').textContent = S.code;
    const [L, R] = seatPids();
    fillSeat($('#seatL'), L, 'a');
    fillSeat($('#seatR'), R, 'b');
    const venn = $('#seatVenn');
    venn.classList.toggle('apart', !R);
    if (lastSeatR === null && R && !reduced) {
      const seat = $('#seatR');
      seat.classList.remove('arrive'); void seat.offsetWidth; seat.classList.add('arrive');
      setTimeout(() => seat.classList.remove('arrive'), 900);
      venn.classList.remove('bump'); void venn.offsetWidth; venn.classList.add('bump');
      vibrate(20);
    }
    lastSeatR = R || null;

    const spectator = !(S.mine && S.cfg.players.includes(S.mine.pid));
    const lim = S.cfg.limit | 0;
    const seg = $('#limitSeg');
    seg.style.setProperty('--idx', lim === 30 ? 0 : lim === 60 ? 1 : 2);
    $$('button', seg).forEach(b => {
      const on = (+b.dataset.v) === lim;
      b.classList.toggle('on', on);
      b.setAttribute('aria-checked', on);
      b.disabled = spectator;
    });
    $('#noRepeat').checked = !!S.cfg.noRepeat;
    $('#noRepeat').disabled = spectator;

    const full = S.cfg.players.length >= 2;
    const start = $('#startBtn');
    start.disabled = !full || spectator;
    start.classList.toggle('ready', full && !spectator);
    start.classList.toggle('hidden', spectator);
    let hint;
    if (spectator) hint = t('hintWatch');
    else if (!full) hint = t('hintAlone');
    else if (!isOnline(R)) hint = t('hintAway', nameOf(R));
    else hint = t('hintReady');
    $('#lobbyHint').textContent = hint;
  }

  function renderSeats() {
    const ps = (S.cfg && S.cfg.players) || [];
    $('#seatList').innerHTML = ps.map((pid, i) => {
      const on = isOnline(pid);
      const nm = nameOf(pid, t('playerN', i + 1));
      return `<button type="button" class="seat-pick" data-pid="${esc(pid)}">
        <span class="avatar" style="background:var(--${i ? 'b' : 'a'})">${esc(initial(nm))}<i class="dot${on ? ' on' : ''}"></i></span>
        <span class="grow"><span class="who">${esc(t('iAm', nm))}</span><span class="st">${esc(on ? t('seatOnline') : t('away'))}</span></span>
        ${icon('i-chevron')}</button>`;
    }).join('');
  }

  // ----- 历史记录（增量渲染） -----
  const H = {
    gid: null, n: 0, L: null, fontWait: false, silent: true,
    // silent：重建（如切换语言、刚进房间）时不重放揭晓动画
    reset() { this.gid = null; this.n = 0; this.L = null; this.silent = true; const box = $('#history'); if (box) box.innerHTML = ''; },
  };
  const wordHtml = w => w ? esc(w) : `<span class="blank">${esc(t('blank'))}</span>`;
  function fontsReadyFor(text) {
    try { return !document.fonts || document.fonts.check('600 20px "LX Serif"', text); } catch { return true; }
  }

  function roundEl(i, a, b, isMatch, live) {
    const el = document.createElement('div');
    if (isMatch) {
      el.className = 'round match' + (live ? ' live' : '');
      el.innerHTML = `<div class="mrow">
          <div class="word a ghost">${wordHtml(a)}</div>
          <div class="rno ghost">${i + 1}</div>
          <div class="word b ghost">${wordHtml(b)}</div>
          <div class="word merged"><span>${esc(a)}</span><i class="sheen"></i></div>
        </div>
        <div class="tag">${icon('i-spark')}<span>${esc(t('matchTag', i + 1))}</span></div>`;
    } else {
      el.className = 'round' + (live ? ' reveal' : '');
      el.innerHTML = `<div class="word a">${wordHtml(a)}</div><div class="rno">${i + 1}</div><div class="word b">${wordHtml(b)}</div>`;
    }
    return el;
  }

  // 返回本次是否"现场揭晓"了新的轮次
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
        empty.innerHTML = `<div class="venn pulse" aria-hidden="true"><i></i><i></i></div><b>${esc(t('emptyTitle'))}</b><div>${t('emptyText')}</div>`;
        box.appendChild(empty);
      }
      H.silent = false;
      return false;
    }
    if (H.n >= d.rounds) { H.silent = false; return false; }
    const live = S.ready && Date.now() - S.readyAt > 1200 && !H.silent;
    // 新词的字形还没下载好时，稍等一下再揭晓，避免字体闪变
    const text = [];
    for (let i = H.n; i < d.rounds; i++) text.push(d.A[i] || '', d.B[i] || '');
    if (H.fontWait) return false;
    if (live && !fontsReadyFor(text.join(''))) {
      H.fontWait = true;
      const gen = S.gen;
      Promise.race([document.fonts.load('600 20px "LX Serif"', text.join('')), sleep(900)])
        .catch(() => { }).then(() => { H.fontWait = false; if (gen === S.gen) render(); });
      return false;
    }
    if (empty) empty.remove();
    for (let i = H.n; i < d.rounds; i++) box.appendChild(roundEl(i, d.A[i], d.B[i], i === d.match, live));
    H.n = d.rounds;
    H.silent = false;
    requestAnimationFrame(() => box.scrollTo({ top: box.scrollHeight, behavior: live && !reduced ? 'smooth' : 'auto' }));
    if (live) vibrate(d.match >= 0 ? [30, 70, 30] : 18);
    return live;
  }

  function setPane(name) {
    for (const p of $$('#stage .pane')) {
      const on = p.dataset.pane === name;
      if (p.classList.contains('on') !== on) p.classList.toggle('on', on);
    }
  }

  let lastPrompt = null, lastTitle = null, lastPStatus = null;
  function setPrompt(html) {
    const el = $('#prompt');
    if (html === lastPrompt) return;
    const had = lastPrompt != null;
    lastPrompt = html;
    el.innerHTML = html;
    if (had && !reduced) { el.classList.remove('swap'); void el.offsetWidth; el.classList.add('swap'); }
  }
  const wspan = (w, cls) => `<span class="w ${cls}">${esc(w)}</span>`;

  function renderGame() {
    const d = derive();
    const live = renderHistory(d);
    const partner = nameOf(d.R, d.isPlayer ? t('partner') : t('playerN', 2));

    // 顶栏
    const title = d.match >= 0 ? t('syncTitle') : t('round', d.cur + 1);
    const titleEl = $('#roundTitle');
    if (title !== lastTitle) {
      titleEl.textContent = title;
      if (lastTitle != null && !reduced) { titleEl.classList.remove('flip'); void titleEl.offsetWidth; titleEl.classList.add('flip'); }
      lastTitle = title;
    }
    $('#gameCode').textContent = S.code;

    // 名字栏
    $('#nameL').textContent = nameOf(d.L, t('playerN', 1));
    $('#nameR').textContent = nameOf(d.R, t('playerN', 2));
    $('#meL').textContent = t('me');
    $('#meL').classList.toggle('hidden', !d.isPlayer);
    $('#dotL').classList.toggle('on', isOnline(d.L));
    $('#dotR').classList.toggle('on', isOnline(d.R));
    $('#menuEnd').classList.toggle('hidden', !d.isPlayer);

    const pst = $('#pStatus');
    const setStatus = (cls, html) => {
      const key = cls + '|' + html;
      if (key === lastPStatus) return;
      lastPStatus = key;
      pst.className = 'pstatus ' + cls;
      pst.innerHTML = html;
      if (!reduced) { pst.classList.remove('swap'); void pst.offsetWidth; pst.classList.add('swap'); }
    };

    if (d.match >= 0) {
      const w = d.A[d.match];
      setPrompt(t('doneLine', `<span class="w m">${esc(w)}</span>`));
      $('#againBtn2').classList.toggle('hidden', !d.isPlayer);
      setPane('done');
      setStatus('', '');
      if (S.resultFor !== S.cfg.gid) {
        S.resultFor = S.cfg.gid;
        if (live) {
          const gid = S.cfg.gid;
          setTimeout(() => { if (S.cfg && S.cfg.gid === gid) confettiFrom($('#history .round.match .merged')); }, reduced ? 0 : 1150);
          setTimeout(() => { if (S.cfg && S.cfg.gid === gid && view() === 'game') openResult(true); }, reduced ? 300 : 2500);
        } else openResult(false);
      }
      return;
    }

    // 提示语
    if (d.cur === 0) setPrompt(esc(t('promptFirst')));
    else {
      const pa = d.A[d.cur - 1], pb = d.B[d.cur - 1];
      if (!pa && !pb) setPrompt(esc(t('promptBlank')));
      else if (!pa || !pb) setPrompt(t('promptOne', wspan(pa || pb, pa ? 'a' : 'b')));
      else setPrompt(t('promptBridge', wspan(pa, 'a'), wspan(pb, 'b')));
    }

    if (!d.isPlayer) {
      const s = pid => (wordsOf(pid).length > d.done ? t('stReady') : (isOnline(pid) ? t('stThinking') : t('stAway')));
      $('#watchText').textContent = t('watchLine', nameOf(d.L, t('playerN', 1)), s(d.L), nameOf(d.R, t('playerN', 2)), s(d.R));
      setPane('watch');
      setStatus('', '');
      return;
    }

    if (!d.lSub) {
      setPane('input');
      const overlayOpen = !$('#result').classList.contains('hidden') || !$('#dialog').classList.contains('hidden') || !$('#menu').classList.contains('hidden');
      const input = $('#wordInput');
      if (!isTouch && !overlayOpen && document.activeElement !== input) input.focus({ preventScroll: true });
    } else {
      const mine = d.A[d.done];
      $('#sealedWord').textContent = mine || t('blank');
      $('#sealedText').textContent = t('sealedText', partner);
      setPane('sealed');
    }

    if (!d.R) setStatus('', esc(t('noPartner')));
    else if (d.rSub) setStatus('ready', svgCheck + esc(t('partnerReady', partner)));
    else if (isOnline(d.R)) setStatus('', `<i class="dot on"></i>${esc(t('partnerThinking', partner))}<span class="typing"><i></i><i></i><i></i></span>`);
    else setStatus('', `<i class="dot"></i>${esc(t('partnerAway', partner))}`);
  }

  // ----- 结果 -----
  function fillResult(d) {
    if (d.match < 0) return;
    const w = d.A[d.match];
    $('#resultKicker').textContent = t('resultKicker', d.match + 1);
    $('#resultWord').textContent = w;
    $('#resultSub').textContent = d.match === 0 ? t('resultFirst') : t('resultSub', nameOf(d.L, t('playerN', 1)), nameOf(d.R, t('playerN', 2)), d.match + 1);
    const chainWord = x => x ? esc(x) : `<span class="blank">${esc(t('blank'))}</span>`;
    $('#chain').innerHTML = Array.from({ length: d.match + 1 }, (_, i) => i === d.match
      ? `<li class="m"><span class="i">${i + 1}</span><span class="x"><span>${esc(d.A[i])}</span>${icon('i-spark')}</span></li>`
      : `<li><span class="i">${i + 1}</span><span class="x a">${chainWord(d.A[i])}</span><span class="x b">${chainWord(d.B[i])}</span></li>`
    ).join('');
    $('#againBtn').classList.toggle('hidden', !d.isPlayer);
  }
  function openResult(play) {
    const d = derive();
    if (d.match < 0) return;
    fillResult(d);
    const el = $('#result');
    el.classList.remove('play', 'settled', 'closing', 'hidden');
    void el.offsetWidth;
    el.classList.add(play && !reduced ? 'play' : 'settled');
    S.resultOpen = true;
  }

  function openOverlay(sel) {
    const el = $(sel);
    el.classList.remove('closing', 'hidden');
  }
  function closeOverlay(sel, instant) {
    const el = $(sel);
    if (el.classList.contains('hidden')) return;
    if (sel === '#result') S.resultOpen = false;
    if (instant || reduced) { el.classList.add('hidden'); el.classList.remove('closing', 'play', 'settled'); return; }
    el.classList.add('closing');
    clearTimeout(el._t);
    el._t = setTimeout(() => { el.classList.add('hidden'); el.classList.remove('closing', 'play', 'settled'); }, 220);
  }

  function resultText() {
    const d = derive();
    if (d.match < 0) return '';
    const lines = [t('recapHead', d.match + 1, d.A[d.match]), ''];
    for (let i = 0; i < d.match; i++) lines.push(`${i + 1}. ${d.A[i] || '—'} / ${d.B[i] || '—'}`);
    lines.push(`${d.match + 1}. ${d.A[d.match]} = ${d.B[d.match]}`);
    lines.push('', t('recapFoot') + location.origin + location.pathname);
    return lines.join('\n');
  }

  function updateConn() {
    const n = S.net ? S.net.clients.filter(c => c && c.connected).length : 0;
    const total = S.net ? S.net.clients.filter(Boolean).length : 0;
    for (const el of $$('[data-conn]')) {
      el.className = 'conn' + (n > 0 ? ' ok' : (S.net ? ' ing' : ' bad'));
      el.title = n > 0 ? t('connectedN', n, total) : t('connecting');
    }
    const tx = $('#connTxt');
    if (tx) tx.textContent = n > 0 ? (total > 1 ? t('connectedN', n, total) : t('connected')) : t('connecting');
  }

  // ---------- 小组件 ----------
  let toastTimer;
  function toast(msg) {
    const el = $('#toast');
    el.textContent = msg;
    el.classList.remove('show'); void el.offsetWidth; el.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => el.classList.remove('show'), 2600);
  }
  function shake(sel) {
    const el = $(sel);
    if (!el || reduced) return;
    el.animate([{ transform: 'translateX(0)' }, { transform: 'translateX(-7px)' }, { transform: 'translateX(6px)' }, { transform: 'translateX(-4px)' }, { transform: 'translateX(2px)' }, { transform: 'translateX(0)' }], { duration: 420, easing: 'ease-out' });
  }
  function vibrate(p) { try { navigator.vibrate && navigator.vibrate(p); } catch { } }

  function confirmDlg(title, text, okText, cancelText) {
    return new Promise(resolve => {
      $('#dlgTitle').textContent = title;
      $('#dlgText').textContent = text;
      $('#dlgOk').textContent = okText;
      $('#dlgCancel').textContent = cancelText || t('cancel');
      openOverlay('#dialog');
      const done = v => { closeOverlay('#dialog'); $('#dlgOk').onclick = $('#dlgCancel').onclick = $('#dialog').onclick = null; resolve(v); };
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
  const inviteLink = () => `${location.origin}${location.pathname}${location.search}#${S.code}`;

  // 彩带（从合一的那张卡片里迸发）
  function confettiFrom(el) {
    if (reduced) return;
    const cv = $('#confetti'), ctx = cv.getContext('2d');
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const W = cv.width = innerWidth * dpr, Hh = cv.height = innerHeight * dpr;
    let ox = W / 2, oy = Hh * .45;
    if (el) { const r = el.getBoundingClientRect(); ox = (r.left + r.width / 2) * dpr; oy = (r.top + r.height / 2) * dpr; }
    const css = getComputedStyle(document.documentElement);
    const colors = ['--a', '--b', '--mix'].map(v => css.getPropertyValue(v).trim()).concat(['#F3C15B', '#FFFFFF']);
    const parts = Array.from({ length: 120 }, () => {
      const ang = -Math.PI / 2 + (Math.random() - .5) * Math.PI * 1.25;
      const sp = (Math.random() * 13 + 7) * dpr;
      return {
        x: ox + (Math.random() - .5) * 60 * dpr, y: oy,
        vx: Math.cos(ang) * sp, vy: Math.sin(ang) * sp,
        s: (Math.random() * 5 + 5) * dpr, r: Math.random() * 6, vr: (Math.random() - .5) * .28,
        w: Math.random() * Math.PI * 2,
        c: colors[(Math.random() * colors.length) | 0], heart: Math.random() < .28,
      };
    });
    const t0 = performance.now();
    (function frame(now) {
      const el2 = now - t0;
      ctx.clearRect(0, 0, W, Hh);
      for (const p of parts) {
        p.vy += .42 * dpr; p.vx *= .983; p.vy *= .983;
        p.x += p.vx + Math.sin(p.w += .08) * .6 * dpr; p.y += p.vy; p.r += p.vr;
        ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.r);
        ctx.globalAlpha = Math.max(0, 1 - Math.max(0, el2 - 1400) / 1800);
        ctx.fillStyle = p.c;
        if (p.heart) {
          const s = p.s;
          ctx.beginPath(); ctx.moveTo(0, s * .32);
          ctx.bezierCurveTo(-s, -s * .38, -s * .38, -s, 0, -s * .36);
          ctx.bezierCurveTo(s * .38, -s, s, -s * .38, 0, s * .32);
          ctx.fill();
        } else {
          const sy = Math.abs(Math.cos(p.w)) * .5 + .15;
          ctx.fillRect(-p.s / 2, -p.s * sy / 2, p.s, p.s * sy);
        }
        ctx.restore();
      }
      if (el2 < 3300) requestAnimationFrame(frame); else ctx.clearRect(0, 0, W, Hh);
    })(t0);
  }

  // 字体：预加载常用分片；首屏标题等字体就绪再出现
  function preloadFonts(n) {
    if (!document.fonts || !document.fonts.load) return;
    if ((preloadFonts.done || 0) >= n) return;
    const from = preloadFonts.done || 0;
    preloadFonts.done = n;
    FONT_PROBES.slice(from, n).reduce((p, ch) => p.then(() => document.fonts.load('600 20px "LX Serif"', ch)).catch(() => { }), Promise.resolve());
  }
  function revealWhenFontsReady() {
    const html = document.documentElement;
    if (!document.fonts || !document.fonts.load) { html.classList.remove('fonts-pending'); return; }
    const need = LANG === 'en'
      ? [document.fonts.load('600 52px "LX Serif"', 'Kindred'), document.fonts.load('italic 400 18px "LX Serif"', 'Two minds')]
      : [document.fonts.load('600 44px "LX Serif"', '心有灵犀')];
    Promise.race([
      Promise.all(need),
      sleep(1500),
    ]).catch(() => { }).then(() => html.classList.remove('fonts-pending'));
  }

  // 视口高度（处理手机键盘）
  function fitViewport() {
    const h = window.visualViewport ? window.visualViewport.height : innerHeight;
    document.documentElement.style.setProperty('--app-h', h + 'px');
    if (view() === 'game') window.scrollTo(0, 0);
  }

  // 输入法（中文拼音等）组字期间按回车，不应触发提交
  function imeGuard(input) {
    let composing = false, endAt = 0;
    input.addEventListener('compositionstart', () => { composing = true; });
    input.addEventListener('compositionend', () => { composing = false; endAt = performance.now(); });
    return e => composing || e.isComposing || e.keyCode === 229 || performance.now() - endAt < 40;
  }

  // ---------- 事件 ----------
  function bind() {
    const nameInput = $('#nameInput'), codeInput = $('#codeInput'), wordInput = $('#wordInput');
    nameInput.value = store.get('name', '');
    const getName = () => {
      const n = clip(nameInput.value, MAX_NAME);
      if (!n) { shake('#nameInput'); nameInput.focus(); toast(t('tNeedName')); return null; }
      store.set('name', n);
      return n;
    };

    $$('.lang button').forEach(b => b.addEventListener('click', () => setLang(b.dataset.lang)));

    $('#createBtn').onclick = () => { const n = getName(); if (n) enterRoom(newCode(), n, 'create'); };
    const join = () => {
      const c = cleanCode(codeInput.value);
      if (!validCode(c)) { shake('.join-row'); toast(t('tBadCode')); codeInput.focus(); return; }
      const n = getName(); if (n) enterRoom(c, n, 'join');
    };
    $('#joinBtn').onclick = join;
    codeInput.addEventListener('input', () => { codeInput.value = cleanCode(codeInput.value).slice(0, CODE_LEN); });
    const nameIme = imeGuard(nameInput), codeIme = imeGuard(codeInput), wordIme = imeGuard(wordInput);
    codeInput.addEventListener('keydown', e => { if (e.key === 'Enter' && !codeIme(e)) { e.preventDefault(); join(); } });
    nameInput.addEventListener('keydown', e => {
      if (e.key !== 'Enter' || nameIme(e)) return;
      e.preventDefault();
      if (cleanCode(codeInput.value).length === CODE_LEN) join(); else $('#createBtn').click();
    });
    $('#resumeBtn').onclick = () => { const r = resumeTarget(); if (r) enterRoom(r.code, null, 'resume', r.pid); };

    const rules = $('#rules');
    $('#rulesToggle').onclick = () => {
      const open = !rules.classList.contains('open');
      rules.classList.toggle('open', open);
      $('#rulesToggle').setAttribute('aria-expanded', open);
    };

    document.addEventListener('click', async e => {
      const a = e.target.closest('[data-act="leave"]');
      if (!a) return;
      const d = view() === 'game' ? derive() : null;
      if (d && d.match < 0 && d.isPlayer) {
        if (!await confirmDlg(t('leaveTitle'), t('leaveText'), t('leave'), t('stay'))) return;
      }
      leaveRoom();
    });

    $('#roomCode').onclick = async () => {
      const el = $('#roomCode');
      el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');
      if (await copyText(S.code)) toast(t('codeCopied'));
    };
    const shareInvite = async () => {
      const url = inviteLink();
      const text = t('inviteText', S.code);
      if (navigator.share && isTouch) {
        try { await navigator.share({ title: t('appTitle'), text, url }); return; } catch (e) { if (e && e.name === 'AbortError') return; }
      }
      if (await copyText(`${text}\n${url}`)) toast(t('inviteCopied'));
      else toast(url);
    };
    $('#shareBtn').onclick = shareInvite;
    $('#limitSeg').onclick = e => {
      const b = e.target.closest('button'); if (!b || b.disabled) return;
      if ((+b.dataset.v) !== (S.cfg.limit | 0)) writeCfg({ limit: +b.dataset.v });
    };
    $('#noRepeat').onchange = e => writeCfg({ noRepeat: e.target.checked });
    $('#startBtn').onclick = () => {
      if (!S.cfg || S.cfg.players.length < 2) return;
      writeCfg({ phase: 'play', gid: rand(8), startedAt: Date.now() });
    };
    $('#nfCreate').onclick = () => { S.mode = 'create'; createCfg(); };

    $('#seatList').onclick = e => { const b = e.target.closest('[data-pid]'); if (b) takeSeat(b.dataset.pid); };
    $('#spectateBtn').onclick = spectate;

    $('#wordForm').addEventListener('submit', e => { e.preventDefault(); trySubmit(wordInput.value); });
    wordInput.addEventListener('keydown', e => {
      if (e.key !== 'Enter') return;
      e.preventDefault();
      if (!wordIme(e)) trySubmit(wordInput.value);
    });

    const again = () => { closeOverlay('#result'); writeCfg({ phase: 'lobby' }); };
    $('#againBtn').onclick = again;
    $('#againBtn2').onclick = again;
    $('#closeResult').onclick = () => closeOverlay('#result');
    $('#result').onclick = e => { if (e.target.id === 'result') closeOverlay('#result'); };
    $('#showResultBtn').onclick = () => openResult(false);
    $('#copyBtn').onclick = async () => { if (await copyText(resultText())) toast(t('recapCopied')); };

    $('#menuBtn').onclick = () => openOverlay('#menu');
    $('#menu').onclick = e => { if (e.target.id === 'menu') closeOverlay('#menu'); };
    $('#menuCancel').onclick = () => closeOverlay('#menu');
    $('#menuInvite').onclick = () => { closeOverlay('#menu'); shareInvite(); };
    $('#menuEnd').onclick = async () => {
      closeOverlay('#menu');
      await sleep(180);
      if (await confirmDlg(t('endTitle'), t('endText'), t('endGame'))) writeCfg({ phase: 'lobby' });
    };

    document.addEventListener('keydown', e => {
      if (e.key !== 'Escape') return;
      for (const s of ['#dialog', '#menu', '#result']) if (!$(s).classList.contains('hidden')) { if (s === '#dialog') $('#dlgCancel').click(); else closeOverlay(s); break; }
    });

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
      $('#joinBtn').classList.add('primary');
      $('#createBtn').classList.remove('primary');
      render();
      const nameInput = $('#nameInput');
      if (!nameInput.value && !isTouch) setTimeout(() => nameInput.focus(), 400);
      toast(t('tInvite', c));
      return;
    }
    render();
  }

  // 定时刷新：计时器 / 在线状态
  setInterval(tick, 200);
  setInterval(() => { if (S.code && view() !== 'game') render(); }, 3000);

  applyStatic();
  fitViewport();
  bind();
  revealWhenFontsReady();
  boot();
  setTimeout(() => preloadFonts(3), 1500);

  window.__lingxi = { S, derive, setLang, same };
})();
