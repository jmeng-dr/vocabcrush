// 心有灵犀 · 配置
// 网站本身是纯静态页面（可直接放在 GitHub Pages 上），
// 两人之间的实时同步通过公共 MQTT 服务器（WebSocket + TLS）转发。
// 所有游戏数据在浏览器里用房间号派生的密钥加密（AES-GCM），服务器只能看到密文。
//
// 页面会同时连接下面所有服务器，只要两人有任意一条线路相通就能玩，
// 某个服务器挂了/被墙了也不影响。可以按需增删（必须是 wss:// 地址）。
window.LINGXI_CONFIG = {
  brokers: [
    'wss://broker.emqx.io:8084/mqtt',     // EMQX 公共服务器（国内外均可访问）
    'wss://broker.hivemq.com:8884/mqtt',  // HiveMQ 公共服务器（备用线路）
  ],
  // 主题前缀，一般不用改。如果你想和别人的部署完全隔离，可以改成任意字符串。
  namespace: 'lingxi/v1',
};
