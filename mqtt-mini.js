/* Minimal MQTT 3.1.1 over WebSocket client (QoS 0 only). ES5 for old Samsung TVs. */
function MiniMQTT(opts) {
  var self = this;
  self.brokers = opts.brokers;
  self.bi = 0;
  self.topic = opts.topic;
  self.onMessage = opts.onMessage || function () {};
  self.onState = opts.onState || function () {};
  self.ws = null;
  self.buf = [];
  self.ready = false;
  self.pid = 1;
  self.ping = null;
  self.closed = false;
  self.clientId = 'fs' + Math.random().toString(36).slice(2, 12);
  self.connect();
}

MiniMQTT.utf8 = function (s) {
  var out = [], i, c;
  s = unescape(encodeURIComponent(s));
  for (i = 0; i < s.length; i++) { c = s.charCodeAt(i); out.push(c); }
  return out;
};
MiniMQTT.fromUtf8 = function (bytes) {
  var s = '', i;
  for (i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  try { return decodeURIComponent(escape(s)); } catch (e) { return s; }
};
MiniMQTT.str = function (s) {
  var b = MiniMQTT.utf8(s);
  return [b.length >> 8, b.length & 255].concat(b);
};
MiniMQTT.packet = function (type, body) {
  var len = body.length, head = [type];
  do {
    var d = len % 128; len = Math.floor(len / 128);
    if (len > 0) d = d | 128;
    head.push(d);
  } while (len > 0);
  return new Uint8Array(head.concat(body));
};

MiniMQTT.prototype.connect = function () {
  var self = this;
  if (self.closed) return;
  var url = self.brokers[self.bi % self.brokers.length];
  self.onState('connecting');
  try { self.ws = new WebSocket(url, 'mqtt'); } catch (e) { return self.retry(); }
  self.ws.binaryType = 'arraybuffer';
  self.buf = [];
  self.ws.onopen = function () {
    var body = MiniMQTT.str('MQTT').concat([4, 2, 0, 60]).concat(MiniMQTT.str(self.clientId));
    self.ws.send(MiniMQTT.packet(0x10, body));
  };
  self.ws.onmessage = function (ev) {
    var a = new Uint8Array(ev.data), i;
    for (i = 0; i < a.length; i++) self.buf.push(a[i]);
    self.parse();
  };
  self.ws.onclose = function () { self.retry(); };
  self.ws.onerror = function () { try { self.ws.close(); } catch (e) {} };
};

MiniMQTT.prototype.retry = function () {
  var self = this;
  self.ready = false;
  if (self.ping) { clearInterval(self.ping); self.ping = null; }
  if (self.closed) return;
  self.onState('offline');
  self.bi++;
  setTimeout(function () { self.connect(); }, 3000);
};

MiniMQTT.prototype.parse = function () {
  var self = this;
  while (self.buf.length >= 2) {
    var mult = 1, len = 0, i = 1, d;
    do {
      if (i >= self.buf.length) return;
      d = self.buf[i++];
      len += (d & 127) * mult; mult *= 128;
    } while (d & 128);
    if (self.buf.length < i + len) return;
    var type = self.buf[0] >> 4, qos = (self.buf[0] >> 1) & 3;
    var body = self.buf.slice(i, i + len);
    self.buf = self.buf.slice(i + len);
    if (type === 2) { // CONNACK
      self.ready = true;
      self.onState('online');
      if (self.topic) {
        var sb = [0, (self.pid++) & 255].concat(MiniMQTT.str(self.topic)).concat([0]);
        self.ws.send(MiniMQTT.packet(0x82, sb));
      }
      self.ping = setInterval(function () {
        try { self.ws.send(new Uint8Array([0xC0, 0])); } catch (e) {}
      }, 30000);
    } else if (type === 3) { // PUBLISH
      var tl = (body[0] << 8) | body[1];
      var topic = MiniMQTT.fromUtf8(body.slice(2, 2 + tl));
      var p = 2 + tl + (qos > 0 ? 2 : 0);
      self.onMessage(topic, MiniMQTT.fromUtf8(body.slice(p)));
    }
  }
};

MiniMQTT.prototype.publish = function (topic, text) {
  if (!this.ready) return false;
  var body = MiniMQTT.str(topic).concat(MiniMQTT.utf8(text));
  this.ws.send(MiniMQTT.packet(0x30, body));
  return true;
};

MiniMQTT.prototype.close = function () {
  this.closed = true;
  try { this.ws.close(); } catch (e) {}
};

var FS_BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt',
  'wss://test.mosquitto.org:8081/mqtt'
];
