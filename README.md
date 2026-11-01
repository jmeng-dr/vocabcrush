# 心有灵犀 · 双人联想游戏

两个人各写一个词，同时揭晓；下一轮根据上一轮的两个词继续联想，直到两人写出**同一个词**。

- 两人远程实时对战，发个链接就能玩
- 每轮限时：30 秒 / 60 秒 / 不限时（时间到会自动提交输入框里的内容）
- 每轮结束同时揭晓两人的词，所有轮次都保留展示
- 默认不能写已经出现过的词（房间里可关闭）
- 刷新、断网、切后台都能自动恢复进度；换手机可以"认领"自己的座位
- 第三个人打开链接可以观战
- 手机优先设计，自动适配深色模式

纯静态网站，**不需要服务器、不需要注册任何服务**，直接放到 GitHub Pages 就能用。

---

## 部署到 GitHub Pages（约 3 分钟）

### 方法一：网页上传（不用装任何东西）

1. 登录 GitHub，右上角 **+ → New repository**，名字随便起（比如 `lingxi`），选 **Public**，点 **Create repository**。
2. 在新仓库页面点 **uploading an existing file**，把本文件夹里的**所有文件**一起选上传上去（都在同一层，没有子文件夹，手机上也能一次全选），点 **Commit changes**。
3. 进入仓库 **Settings → Pages**，Source 选 **Deploy from a branch**，Branch 选 **main** 和 **/(root)**，点 **Save**。
4. 等 1～2 分钟，页面顶部会出现网址：`https://你的用户名.github.io/lingxi/`，打开就能玩。

### 方法二：命令行

```bash
cd xinyou-lingxi
git init && git add . && git commit -m "心有灵犀"
git branch -M main
git remote add origin https://github.com/你的用户名/lingxi.git
git push -u origin main
```

然后同上第 3 步打开 Pages。

---

## 怎么玩

1. 一个人打开网址，填昵称，点 **创建房间**。
2. 点 **发送邀请链接给 TA**（或者直接告诉 TA 6 位房间号）。
3. TA 打开链接，填昵称，点 **加入**。
4. 任意一方选好限时，点 **开始游戏**。

> 想自己先试试？同一个浏览器开两个标签页，用不同昵称分别创建 / 加入即可。

---

## 原理 & 隐私

GitHub Pages 只能放静态网页，所以两人之间的实时同步借助了**公共 MQTT 服务器**（EMQX、HiveMQ 提供的免费 WebSocket 服务）转发消息：

- 页面会**同时连接两个服务器**，任意一条线路通就能玩，一个挂了另一个顶上。
- 所有游戏内容在浏览器里用**房间号派生的密钥加密**（PBKDF2 + AES-GCM），服务器和其他人只能看到乱码。
- 进度保存在服务器的"保留消息"和你自己浏览器的本地存储里，任意一方刷新都能恢复；服务器重启丢了数据也会被自动补回。

> 这是给朋友/情侣玩的小游戏，加密足以防止路人偷看，但没法防住正在和你玩的人用开发者工具提前看答案 😉

### 换成自己的服务器（可选）

编辑 `config.js` 里的 `brokers` 列表即可，任何支持 **MQTT over WebSocket (wss://)** 的服务器都行，例如：

- 自己用 Docker 跑一个 EMQX / Mosquitto
- EMQX Cloud Serverless、HiveMQ Cloud 的免费套餐（需要的话在 `app.js` 的 `mqtt.connect` 选项里加 `username` / `password`）

---

## 常见问题

**一直显示"连接中"？**
公共服务器偶尔会抽风或被所在网络拦截。换个网络（Wi-Fi ↔ 流量）试试，或在 `config.js` 里加一个新的服务器地址（两人的页面要用同一份配置）。

**微信里打不开 / 提示"非微信官方网页"？**
点右上角 **···** → **在浏览器打开**。

**换了手机怎么继续？**
用同一个链接打开，会看到"房间已满"，点 **我是 XXX** 就能接着玩。

**本地预览**
`crypto.subtle` 需要安全环境，请用本地服务器而不是直接双击打开：

```bash
python3 -m http.server 8080
# 打开 http://localhost:8080
```

---

## 文件说明

| 文件 | 说明 |
| --- | --- |
| `index.html` | 页面结构 |
| `style.css` | 样式（浅色 / 深色） |
| `app.js` | 游戏逻辑与实时同步 |
| `config.js` | 服务器配置 |
| `mqtt.min.js` | [MQTT.js](https://github.com/mqttjs/MQTT.js) 5.x（MIT），本地打包，避免 CDN 在国内访问不稳 |
| `favicon.svg` | 图标 |
| `.nojekyll` | 让 GitHub Pages 原样发布文件（可选，手机上传不了隐藏文件也没关系） |
