/*
 * tt-net.js — browser-side network + voice layer for Turf Trade (grey-box prototype).
 * Exposes window.TT. Needs window.Trystero (trystero-nostr-0.26.0.min.js) loaded first.
 * Called from Unity through Assets/_Project/Plugins/WebGL/TTNet.jslib, or directly from test.html.
 *
 * Rules of this file:
 *   - every public TT.* function is wrapped: it never throws to the caller, it queues {t:"netError",msg}.
 *   - everything Unity needs comes back through polling (TT.pollMsg / TT.pollEvent / TT.pollCommand).
 *   - anything received from another peer is untrusted and validated before use.
 *
 * TODO(proto): no TURN server (some NAT pairs cannot connect), public Nostr relays for signalling,
 *              the lobby directory is a full mesh of everyone browsing it.
 */
(function () {
  'use strict';

  var APP_ID = 'gravity-turf-trade';
  var DIR_ROOM = APP_ID + '-directory';
  var CODE_RE = /^[A-HJKMNP-Z2-9]{5}$/;

  var MAX_MSGS = 2000;          // received game messages kept for pollMsg (oldest dropped)
  var MAX_EVENTS = 2000;        // events kept for pollEvent (oldest dropped)
  var MAX_COMMANDS = 500;
  // Trystero 0.26.0 sends payloads up to 16384-36 bytes as ONE data-channel frame. Anything bigger goes
  // through an offer/accept round trip before the bytes move, which adds a round trip of latency to every
  // big message. So we cut large game messages into inline frames ourselves and reassemble them here.
  var FRAG_BYTES = 15000;
  var MAX_FRAGS = 70;           // ~1 MB hard cap per message
  var MAX_PENDING_SENDS = 256;  // per peer; beyond this the peer is not draining and new messages are dropped
  var PING_EVERY_MS = 3000;
  var DIR_ANNOUNCE_MS = 2000;
  var DIR_EVENT_MS = 2000;
  var DIR_EXPIRE_MS = 6000;
  var DIR_MAX_ENTRIES = 200;
  var MIN_DB = -100;

  var T = window.Trystero || null;

  // ---------------------------------------------------------------- queues

  var events = [];
  var msgs = [];
  var commands = [];
  var stats = { sent: 0, received: 0, droppedIn: 0, droppedOut: 0, fragsIn: 0, fragsOut: 0 };

  function pushEvent(obj) {
    try {
      events.push(JSON.stringify(obj));
      if (events.length > MAX_EVENTS) events.splice(0, events.length - MAX_EVENTS);
    } catch (e) { /* never throw from here */ }
  }

  var lastErrMsg = '';
  var lastErrAt = 0;
  function netError(msg) {
    msg = String(msg);
    var now = Date.now();
    // the same error can repeat at the send rate (15-20 Hz); report it once per second
    if (msg === lastErrMsg && now - lastErrAt < 1000) return;
    lastErrMsg = msg;
    lastErrAt = now;
    if (msg.length > 300) msg = msg.slice(0, 300);
    pushEvent({ t: 'netError', msg: msg });
    try { console.warn('[TT] ' + msg); } catch (e) { }
  }

  function errText(e) {
    return e && e.message ? e.message : String(e);
  }

  function pushMsg(peer, data) {
    msgs.push({ peer: peer, data: data });
    stats.received++;
    if (msgs.length > MAX_MSGS) {
      stats.droppedIn += msgs.length - MAX_MSGS;
      msgs.splice(0, msgs.length - MAX_MSGS);
    }
  }

  function toU8(data) {
    if (data instanceof Uint8Array) return data;
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    return null;
  }

  // ---------------------------------------------------------------- Trystero config

  // Signalling relays (Nostr). Trystero's own default would pick 5 of its 29 built-in relays from the appId;
  // on 2026-11-16 two of those five rejected Trystero's event kinds, so we pin six relays from that same
  // built-in list that were checked to accept and deliver them. Every client must share at least one relay.
  // TODO(proto): public relays come and go. Re-check with the script in README.md if joins start failing.
  var DEFAULT_RELAYS = [
    'wss://relay.flashapp.me',
    'wss://relay.nostr.dev.br',
    'wss://relay.piazza.today',
    'wss://nos.lol',
    'wss://nostrcity-club.fly.dev',
    'wss://relay.aarpia.com'
  ];

  function roomConfig() {
    // window.TT_CONFIG (optional, set before the first join) can override or add e.g.
    //   { relayConfig: { urls: [...] }, turnConfig: [{ urls, username, credential }], rtcConfig: {...} }
    // Trystero reads the relay list once, on the first join of the page.
    var cfg = { appId: APP_ID, relayConfig: { urls: DEFAULT_RELAYS.slice() } };
    var extra = window.TT_CONFIG;
    if (extra && typeof extra === 'object') {
      for (var k in extra) if (Object.prototype.hasOwnProperty.call(extra, k)) cfg[k] = extra[k];
      cfg.appId = APP_ID;
    }
    return cfg;
  }

  function needTrystero() {
    if (!T) T = window.Trystero || null;
    if (!T || typeof T.joinRoom !== 'function') {
      netError('Trystero is not loaded (trystero-nostr-0.26.0.min.js must be included before tt-net.js)');
      return false;
    }
    return true;
  }

  // ---------------------------------------------------------------- game room

  var game = null;   // { room, code, peers:{id:true}, msg, frag, pending:{id:n}, reasm:{id:{...}}, pinging:{id:true} }
  var pings = {};    // peerId -> last round trip in ms
  var pingTimer = null;

  function normCode(code) {
    return String(code == null ? '' : code).trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 16);
  }

  function leaveGame() {
    var g = game;
    if (!g) return;
    game = null;
    msgs.length = 0;
    pings = {};
    for (var id in voices) teardownVoice(id, false);
    peerPos = {};
    peerVol = {};
    try {
      g.room.onPeerJoin = null;
      g.room.onPeerLeave = null;
      g.room.onPeerStream = null;
      g.msg.onMessage = null;
      g.frag.onMessage = null;
    } catch (e) { }
    try {
      var p = g.room.leave();
      if (p && p.catch) p.catch(function () { });
    } catch (e) { netError('leaveRoom: ' + errText(e)); }
  }

  function joinGame(code) {
    leaveGame();
    code = normCode(code);
    if (!code) { netError('joinRoom: empty room code'); return; }
    if (!needTrystero()) return;

    var room = T.joinRoom(roomConfig(), APP_ID + '-' + code, {
      onJoinError: function (d) { netError('join error: ' + (d && d.error ? d.error : 'unknown')); }
    });
    var g = { room: room, code: code, peers: {}, pending: {}, reasm: {}, pinging: {} };
    g.msg = room.makeAction('m');
    g.frag = room.makeAction('f');
    game = g;
    pushEvent({ t: 'roomJoined', code: code });

    g.msg.onMessage = function (data, ctx) {
      if (game !== g) return;
      var u8 = toU8(data);
      if (u8 && ctx && typeof ctx.peerId === 'string') pushMsg(ctx.peerId, u8);
    };
    g.frag.onMessage = function (data, ctx) {
      if (game !== g) return;
      var u8 = toU8(data);
      if (u8 && ctx && typeof ctx.peerId === 'string') onFragment(g, ctx.peerId, u8);
    };

    room.onPeerJoin = function (id) {
      if (game !== g || g.peers[id]) return;
      g.peers[id] = true;
      pushEvent({ t: 'peerJoin', peer: id });
      if (micStream) addMicTo(g, id);
      pingPeer(g, id);
    };
    room.onPeerLeave = function (id) {
      if (game !== g || !g.peers[id]) return;
      delete g.peers[id];
      delete g.pending[id];
      delete g.reasm[id];
      delete g.pinging[id];
      delete pings[id];
      teardownVoice(id, true);
      delete peerPos[id];
      delete peerVol[id];
      pushEvent({ t: 'peerLeave', peer: id });
    };
    room.onPeerStream = function (stream, id) {
      if (game !== g) return;
      attachVoice(id, stream);
    };

    if (!pingTimer) pingTimer = setInterval(pingAll, PING_EVERY_MS);
  }

  function sendTo(g, action, id, u8) {
    var n = g.pending[id] || 0;
    if (n >= MAX_PENDING_SENDS) {
      stats.droppedOut++;
      netError('send: peer ' + id + ' is not draining, dropping messages');
      return;
    }
    g.pending[id] = n + 1;
    var done = function () { if (g.pending[id]) g.pending[id]--; };
    action.send(u8, { target: id }).then(done, function (e) {
      done();
      if (game === g && g.peers[id]) netError('send: ' + errText(e));
    });
  }

  var nextMsgId = 1;

  function sendGame(peerId, u8) {
    var g = game;
    if (!g) return;
    u8 = toU8(u8);
    if (!u8) { netError('send: data must be a Uint8Array'); return; }
    var targets;
    if (peerId) {
      if (!g.peers[peerId]) return;   // unknown or already gone: drop quietly
      targets = [peerId];
    } else {
      targets = Object.keys(g.peers);
      if (!targets.length) return;
    }
    stats.sent++;
    var i, j;
    if (u8.length <= FRAG_BYTES) {
      for (i = 0; i < targets.length; i++) sendTo(g, g.msg, targets[i], u8);
      return;
    }
    var count = Math.ceil(u8.length / FRAG_BYTES);
    if (count > MAX_FRAGS) { netError('send: message too large (' + u8.length + ' bytes)'); return; }
    var id = nextMsgId++ >>> 0;
    for (j = 0; j < count; j++) {
      var part = u8.subarray(j * FRAG_BYTES, Math.min(u8.length, (j + 1) * FRAG_BYTES));
      var frame = new Uint8Array(8 + part.length);   // [msgId u32][index u16][count u16][bytes]
      var dv = new DataView(frame.buffer);
      dv.setUint32(0, id);
      dv.setUint16(4, j);
      dv.setUint16(6, count);
      frame.set(part, 8);
      stats.fragsOut++;
      for (i = 0; i < targets.length; i++) sendTo(g, g.frag, targets[i], frame);
    }
  }

  function onFragment(g, peer, u8) {
    if (u8.length <= 8) return;
    var dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
    var id = dv.getUint32(0), idx = dv.getUint16(4), count = dv.getUint16(6);
    if (count < 2 || count > MAX_FRAGS || idx >= count || u8.length - 8 > FRAG_BYTES) return;
    stats.fragsIn++;
    var st = g.reasm[peer];
    if (idx === 0) {
      st = g.reasm[peer] = { id: id, count: count, next: 0, parts: [], bytes: 0 };
    }
    // the data channel is ordered + reliable, so fragments of one message arrive in order
    if (!st || st.id !== id || st.count !== count || st.next !== idx) { delete g.reasm[peer]; return; }
    var part = u8.subarray(8);
    st.parts.push(part);
    st.bytes += part.length;
    st.next++;
    if (st.next < count) return;
    delete g.reasm[peer];
    var out = new Uint8Array(st.bytes), o = 0;
    for (var i = 0; i < st.parts.length; i++) { out.set(st.parts[i], o); o += st.parts[i].length; }
    pushMsg(peer, out);
  }

  function pingPeer(g, id) {
    if (g.pinging[id]) return;   // one outstanding ping per peer
    g.pinging[id] = true;
    var p;
    try { p = g.room.ping(id); } catch (e) { delete g.pinging[id]; return; }
    p.then(function (ms) {
      delete g.pinging[id];
      if (game === g && g.peers[id]) pings[id] = Math.round(ms);
    }, function () { delete g.pinging[id]; });
  }

  function pingAll() {
    var g = game;
    if (!g) return;
    for (var id in g.peers) pingPeer(g, id);
  }

  // ---------------------------------------------------------------- lobby directory

  var dir = null;   // { room, action, table:{peer:{lobby,at}}, timer, lastSent, lastEvent }
  var dirAnnouncement = null;   // sanitised lobby object this client announces, or null

  function clampInt(v, lo, hi) {
    if (typeof v !== 'number' || !isFinite(v)) return lo;
    v = Math.floor(v);
    return v < lo ? lo : v > hi ? hi : v;
  }

  function cleanStr(v, max) {
    if (typeof v !== 'string') return '';
    // strip control characters, clamp length
    return v.replace(/[\u0000-\u001f\u007f]/g, '').slice(0, max);
  }

  // Returns a clean {code,name,players,max,bots,state} or null when the payload is junk.
  function sanitizeLobby(d) {
    if (!d || typeof d !== 'object' || Array.isArray(d)) return null;
    if (typeof d.code !== 'string' || !CODE_RE.test(d.code)) return null;
    return {
      code: d.code,
      name: cleanStr(d.name, 40),
      players: clampInt(d.players, 0, 64),
      max: clampInt(d.max, 0, 64),
      bots: clampInt(d.bots, 0, 64),
      state: cleanStr(d.state, 24)
    };
  }

  function dirEmit() {
    var d = dir;
    if (!d) return;
    d.lastEvent = Date.now();
    var lobbies = [];
    for (var peer in d.table) {
      var l = d.table[peer].lobby;
      lobbies.push({ code: l.code, name: l.name, players: l.players, max: l.max, bots: l.bots, state: l.state, peer: peer });
    }
    pushEvent({ t: 'dir', lobbies: lobbies });
  }

  function sameLobby(a, b) {
    return a.code === b.code && a.name === b.name && a.players === b.players &&
      a.max === b.max && a.bots === b.bots && a.state === b.state;
  }

  function dirSend(target) {
    var d = dir;
    if (!d || !dirAnnouncement) return;
    var p;
    try {
      p = target ? d.action.send(dirAnnouncement, { target: target }) : d.action.send(dirAnnouncement);
    } catch (e) { netError('dirAnnounce: ' + errText(e)); return; }
    if (p && p.catch) p.catch(function () { });
    if (!target) d.lastSent = Date.now();
  }

  function dirTick() {
    var d = dir;
    if (!d) return;
    var now = Date.now(), changed = false;
    for (var peer in d.table) {
      if (now - d.table[peer].at > DIR_EXPIRE_MS) { delete d.table[peer]; changed = true; }
    }
    if (dirAnnouncement && now - d.lastSent >= DIR_ANNOUNCE_MS - 50) dirSend(null);
    if (changed || now - d.lastEvent >= DIR_EVENT_MS - 50) dirEmit();
  }

  function dirJoin() {
    if (dir) return;
    if (!needTrystero()) return;
    var room = T.joinRoom(roomConfig(), DIR_ROOM, {
      onJoinError: function (d) { netError('directory join error: ' + (d && d.error ? d.error : 'unknown')); }
    });
    var d = { room: room, table: {}, timer: null, lastSent: 0, lastEvent: 0 };
    d.action = room.makeAction('lobby');
    dir = d;

    d.action.onMessage = function (data, ctx) {
      if (dir !== d || !ctx || typeof ctx.peerId !== 'string') return;
      var lobby = sanitizeLobby(data);
      if (!lobby) return;   // junk: ignore
      var peer = ctx.peerId;
      var cur = d.table[peer];
      if (!cur && Object.keys(d.table).length >= DIR_MAX_ENTRIES) return;
      var changed = !cur || !sameLobby(cur.lobby, lobby);
      d.table[peer] = { lobby: lobby, at: Date.now() };
      if (changed) dirEmit();
    };
    room.onPeerJoin = function (id) {
      if (dir !== d) return;
      dirSend(id);   // tell a newcomer about our lobby straight away
    };
    room.onPeerLeave = function (id) {
      if (dir !== d) return;
      if (d.table[id]) { delete d.table[id]; dirEmit(); }
    };
    d.timer = setInterval(dirTick, 500);
    dirEmit();
  }

  function dirLeave() {
    var d = dir;
    if (!d) return;
    dir = null;
    clearInterval(d.timer);
    try {
      d.room.onPeerJoin = null;
      d.room.onPeerLeave = null;
      d.action.onMessage = null;
    } catch (e) { }
    try {
      var p = d.room.leave();
      if (p && p.catch) p.catch(function () { });
    } catch (e) { netError('dirLeave: ' + errText(e)); }
  }

  function dirAnnounce(json) {
    if (json == null || json === '') { dirAnnouncement = null; return; }
    var obj = typeof json === 'string' ? JSON.parse(json) : json;
    var lobby = sanitizeLobby(obj);
    if (!lobby) {
      dirAnnouncement = null;
      netError('dirAnnounce: invalid lobby payload (code must be 5 chars of A-HJKMNP-Z2-9)');
      return;
    }
    dirAnnouncement = lobby;
    dirSend(null);
  }

  // ---------------------------------------------------------------- audio: context, mic, levels

  var ctx = null;          // AudioContext
  var master = null;       // master voice GainNode
  var voiceVolume = 1;

  var micStream = null;
  var micTrack = null;
  var micMeter = null;
  var micFake = null;      // oscillator nodes for TT._fakeMic
  var micPending = false;
  var muted = false;

  var voices = {};         // peerId -> { stream, el, src, gain, panner, meter, onEnded }
  var peerPos = {};        // peerId -> [x, y, z] in Unity space (remembered until the peer leaves)
  var peerVol = {};        // peerId -> 0..2

  function ensureCtx() {
    if (ctx) return ctx;
    var AC = window.AudioContext || window.webkitAudioContext;
    if (!AC) return null;
    ctx = new AC();
    master = ctx.createGain();
    master.gain.value = voiceVolume;
    master.connect(ctx.destination);
    return ctx;
  }

  function resumeCtx() {
    if (ctx && ctx.state !== 'running' && ctx.resume) {
      var p = ctx.resume();
      if (p && p.catch) p.catch(function () { });
    }
  }

  function makeMeter(sourceNode) {
    var an = ctx.createAnalyser();
    an.fftSize = 1024;
    an.smoothingTimeConstant = 0;
    sourceNode.connect(an);
    return { an: an, buf: new Float32Array(an.fftSize), bytes: null, ms: 0, at: 0, db: MIN_DB };
  }

  // RMS of the latest ~21 ms in dBFS, lightly smoothed (mean square moves halfway to the new value per read).
  function meterDb(m) {
    if (!m || !ctx) return MIN_DB;
    var now = performance.now();
    if (now - m.at < 8) return m.db;   // several reads in the same frame give the same answer
    m.at = now;
    var sum = 0, i, n = m.buf.length;
    if (m.an.getFloatTimeDomainData) {
      m.an.getFloatTimeDomainData(m.buf);
      for (i = 0; i < n; i++) sum += m.buf[i] * m.buf[i];
    } else {
      if (!m.bytes) m.bytes = new Uint8Array(n);
      m.an.getByteTimeDomainData(m.bytes);
      for (i = 0; i < n; i++) { var v = (m.bytes[i] - 128) / 128; sum += v * v; }
    }
    var ms = sum / n;
    m.ms += (ms - m.ms) * 0.5;
    var db = m.ms > 1e-10 ? 10 * Math.log10(m.ms) : MIN_DB;
    m.db = db < MIN_DB ? MIN_DB : db > 0 ? 0 : db;
    return m.db;
  }

  function addMicTo(g, peerId) {
    if (!micStream) return;
    try {
      // Trystero 0.26.0 signature: addStream(stream, {target}) -> array of promises
      var ps = g.room.addStream(micStream, { target: peerId });
      if (ps && ps.forEach) ps.forEach(function (p) {
        if (p && p.catch) p.catch(function (e) { netError('addStream: ' + errText(e)); });
      });
    } catch (e) { netError('addStream: ' + errText(e)); }
  }

  function dropMic() {
    var g = game;
    if (micStream && g) { try { g.room.removeStream(micStream); } catch (e) { } }
    if (micStream) { try { micStream.getTracks().forEach(function (t) { t.stop(); }); } catch (e) { } }
    if (micMeter) { try { micMeter.src.disconnect(); } catch (e) { } }
    if (micFake) { try { micFake.osc.stop(); micFake.osc.disconnect(); micFake.gain.disconnect(); } catch (e) { } }
    micStream = null; micTrack = null; micMeter = null; micFake = null;
  }

  function setMic(stream, fake) {
    dropMic();
    micStream = stream;
    micTrack = stream.getAudioTracks()[0] || null;
    micFake = fake || null;
    if (micTrack) {
      micTrack.enabled = !muted;
      micTrack.addEventListener('ended', function () {
        if (micStream === stream) { dropMic(); pushEvent({ t: 'mic', state: 'unavailable' }); }
      });
    }
    if (ensureCtx()) {
      resumeCtx();
      var src = ctx.createMediaStreamSource(stream);
      micMeter = makeMeter(src);
      micMeter.src = src;
    }
    var g = game;
    if (g) for (var id in g.peers) addMicTo(g, id);
    pushEvent({ t: 'mic', state: 'granted' });
  }

  function micLive() {
    return !!(micStream && micTrack && micTrack.readyState === 'live');
  }

  function micStart() {
    if (micLive()) { pushEvent({ t: 'mic', state: 'granted' }); return; }
    if (micPending) return;
    var md = navigator.mediaDevices;
    if (!md || !md.getUserMedia) { pushEvent({ t: 'mic', state: 'unavailable' }); return; }
    micPending = true;
    md.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: false } })
      .then(function (stream) {
        micPending = false;
        try { setMic(stream, null); }
        catch (e) { netError('micStart: ' + errText(e)); pushEvent({ t: 'mic', state: 'unavailable' }); }
      }, function (e) {
        micPending = false;
        var name = e && e.name;
        var denied = name === 'NotAllowedError' || name === 'PermissionDeniedError' || name === 'SecurityError';
        pushEvent({ t: 'mic', state: denied ? 'denied' : 'unavailable' });
      });
  }

  function fakeMic() {
    if (!ensureCtx()) { pushEvent({ t: 'mic', state: 'unavailable' }); return; }
    resumeCtx();
    var osc = ctx.createOscillator();
    var gain = ctx.createGain();
    var dest = ctx.createMediaStreamDestination();
    osc.type = 'sine';
    osc.frequency.value = 440;
    gain.gain.value = 0.25;   // about -15 dBFS RMS
    osc.connect(gain);
    gain.connect(dest);
    osc.start();
    setMic(dest.stream, { osc: osc, gain: gain });
  }

  function setMuted(b) {
    muted = !!b;
    if (micTrack) micTrack.enabled = !muted;
    if (muted && micMeter) { micMeter.ms = 0; micMeter.db = MIN_DB; }
  }

  function micLevelDb() {
    if (!micLive() || muted || !micMeter) return MIN_DB;
    return meterDb(micMeter);
  }

  // ---------------------------------------------------------------- audio: incoming voice, positional

  function clampVol(v) {
    v = Number(v);
    if (!isFinite(v)) return 1;
    return v < 0 ? 0 : v > 2 ? 2 : v;
  }

  // Unity is left-handed (y up, z forward); WebAudio is right-handed: z is negated.
  function placePanner(p, x, y, z) {
    if (p.positionX) {
      p.positionX.value = x; p.positionY.value = y; p.positionZ.value = -z;
    } else if (p.setPosition) {
      p.setPosition(x, y, -z);
    }
  }

  function teardownVoice(peerId, announce) {
    var v = voices[peerId];
    if (!v) return;
    delete voices[peerId];
    try { v.stream.removeEventListener('removetrack', v.onEnded); } catch (e) { }
    try { v.stream.getTracks().forEach(function (t) { t.removeEventListener('ended', v.onEnded); }); } catch (e) { }
    try { v.src.disconnect(); v.gain.disconnect(); v.panner.disconnect(); } catch (e) { }
    try { v.el.pause(); v.el.srcObject = null; } catch (e) { }
    if (announce) pushEvent({ t: 'voicePeer', peer: peerId, on: false });
  }

  function attachVoice(peerId, stream) {
    if (!stream || !stream.getAudioTracks || !stream.getAudioTracks().length) return;
    var old = voices[peerId];
    if (old && old.stream === stream) return;
    teardownVoice(peerId, false);
    if (!ensureCtx()) { netError('voice: WebAudio is not available'); return; }

    // Chrome only starts pulling a remote WebRTC stream into WebAudio once it is also attached to a media
    // element. The element stays muted; the audible path is the WebAudio graph below.
    var el = new Audio();
    el.muted = true;
    el.autoplay = true;
    el.srcObject = stream;
    var pp = el.play();
    if (pp && pp.catch) pp.catch(function () { });

    var src = ctx.createMediaStreamSource(stream);
    var gain = ctx.createGain();
    gain.gain.value = peerVol[peerId] != null ? peerVol[peerId] : 1;
    var panner = ctx.createPanner();
    panner.panningModel = 'HRTF';
    panner.distanceModel = 'inverse';
    panner.refDistance = 2;
    panner.maxDistance = 30;
    panner.rolloffFactor = 1.6;
    var pos = peerPos[peerId];
    if (pos) placePanner(panner, pos[0], pos[1], pos[2]);
    src.connect(gain);
    gain.connect(panner);
    panner.connect(master);
    var meter = makeMeter(src);   // tapped before the volume/panner so it reports what the peer actually sends

    var v = { stream: stream, el: el, src: src, gain: gain, panner: panner, meter: meter, onEnded: null };
    v.onEnded = function () {
      if (voices[peerId] !== v) return;
      var live = stream.getAudioTracks().some(function (t) { return t.readyState === 'live'; });
      if (!live) teardownVoice(peerId, true);
    };
    stream.addEventListener('removetrack', v.onEnded);
    stream.getAudioTracks().forEach(function (t) { t.addEventListener('ended', v.onEnded); });
    voices[peerId] = v;
    pushEvent({ t: 'voicePeer', peer: peerId, on: true });
  }

  function setListener(x, y, z, fx, fy, fz) {
    if (!ctx) return;
    x = +x; y = +y; z = +z; fx = +fx; fy = +fy; fz = +fz;
    if (!isFinite(x + y + z)) return;
    var l = ctx.listener;
    var haveFwd = isFinite(fx + fy + fz) && (fx * fx + fy * fy + fz * fz) > 1e-8;
    if (l.positionX) {
      l.positionX.value = x; l.positionY.value = y; l.positionZ.value = -z;
      if (haveFwd) {
        l.forwardX.value = fx; l.forwardY.value = fy; l.forwardZ.value = -fz;
        l.upX.value = 0; l.upY.value = 1; l.upZ.value = 0;
      }
    } else {
      if (l.setPosition) l.setPosition(x, y, -z);
      if (haveFwd && l.setOrientation) l.setOrientation(fx, fy, -fz, 0, 1, 0);
    }
  }

  function setPeerPos(peerId, x, y, z) {
    x = +x; y = +y; z = +z;
    if (!peerId || !isFinite(x + y + z)) return;
    var pos = peerPos[peerId];
    if (pos) { pos[0] = x; pos[1] = y; pos[2] = z; }
    else if (game && game.peers[peerId]) peerPos[peerId] = [x, y, z];
    var v = voices[peerId];
    if (v) placePanner(v.panner, x, y, z);
  }

  function setPeerVolume(peerId, vol) {
    if (!peerId) return;
    vol = clampVol(vol);
    if (game && game.peers[peerId]) peerVol[peerId] = vol;
    var v = voices[peerId];
    if (v) v.gain.gain.value = vol;
  }

  function setVoiceVolume(vol) {
    voiceVolume = clampVol(vol);
    if (master) master.gain.value = voiceVolume;
  }

  function peerLevelDb(peerId) {
    var v = voices[peerId];
    return v ? meterDb(v.meter) : MIN_DB;
  }

  // ---------------------------------------------------------------- misc

  function copyFallback(s) {
    try {
      var ta = document.createElement('textarea');
      ta.value = s;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.left = '-9999px';
      ta.style.top = '0';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      ta.setSelectionRange(0, s.length);
      document.execCommand('copy');
      document.body.removeChild(ta);
    } catch (e) { netError('copyText: ' + errText(e)); }
  }

  function copyText(s) {
    s = String(s == null ? '' : s);
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(s).catch(function () { copyFallback(s); });
    } else {
      copyFallback(s);
    }
  }

  function getUrlParam(name) {
    var v = new URLSearchParams(window.location.search).get(String(name));
    return v == null ? '' : v;
  }

  function storageGet(key) {
    try {
      var v = window.localStorage.getItem(String(key));
      return v == null ? '' : v;
    } catch (e) { return ''; }
  }

  function storageSet(key, val) {
    try { window.localStorage.setItem(String(key), String(val == null ? '' : val)); } catch (e) { }
  }

  function fullscreen() {
    var d = document, el = d.documentElement;
    var cur = d.fullscreenElement || d.webkitFullscreenElement;
    var p;
    if (cur) p = (d.exitFullscreen || d.webkitExitFullscreen).call(d);
    else p = (el.requestFullscreen || el.webkitRequestFullscreen).call(el);
    if (p && p.catch) p.catch(function (e) { netError('fullscreen: ' + errText(e)); });
  }

  // ---------------------------------------------------------------- public API

  function safe(name, fn, fallback) {
    return function () {
      try { return fn.apply(null, arguments); }
      catch (e) { netError(name + ': ' + errText(e)); return fallback; }
    };
  }

  var TT = {
    // core
    selfId: safe('selfId', function () { return (T || window.Trystero || {}).selfId || ''; }, ''),
    joinRoom: safe('joinRoom', joinGame),
    leaveRoom: safe('leaveRoom', leaveGame),
    send: safe('send', sendGame),
    pollMsg: safe('pollMsg', function () { return msgs.length ? msgs.shift() : null; }, null),
    pollEvent: safe('pollEvent', function () { return events.length ? events.shift() : null; }, null),
    ping: safe('ping', function (peerId) { var v = pings[peerId]; return typeof v === 'number' ? v : -1; }, -1),

    // lobby directory
    dirJoin: safe('dirJoin', dirJoin),
    dirLeave: safe('dirLeave', dirLeave),
    dirAnnounce: safe('dirAnnounce', dirAnnounce),

    // voice
    unlockAudio: safe('unlockAudio', function () { ensureCtx(); resumeCtx(); }),
    micStart: safe('micStart', micStart),
    micLevelDb: safe('micLevelDb', micLevelDb, MIN_DB),
    setMuted: safe('setMuted', setMuted),
    setListener: safe('setListener', setListener),
    setPeerPos: safe('setPeerPos', setPeerPos),
    setPeerVolume: safe('setPeerVolume', setPeerVolume),
    setVoiceVolume: safe('setVoiceVolume', setVoiceVolume),
    peerLevelDb: safe('peerLevelDb', peerLevelDb, MIN_DB),
    _fakeMic: safe('_fakeMic', fakeMic),

    // misc
    copyText: safe('copyText', copyText),
    getUrlParam: safe('getUrlParam', getUrlParam, ''),
    storageGet: safe('storageGet', storageGet, ''),
    storageSet: safe('storageSet', storageSet),
    fullscreen: safe('fullscreen', fullscreen),

    // test hooks
    pollCommand: safe('pollCommand', function () { return commands.length ? commands.shift() : null; }, null),
    setDebugState: safe('setDebugState', function (json) { window.ttState = JSON.parse(json); }),

    // debugging aid (not used by the game): a snapshot of the layer's internal state
    _debug: safe('_debug', function () {
      var relays = {};
      try {
        var socks = T && T.getRelaySockets ? T.getRelaySockets() : {};
        for (var url in socks) relays[url] = socks[url] ? socks[url].readyState : -1;   // 1 = open
      } catch (e) { }
      return {
        selfId: (T || {}).selfId || '',
        room: game ? game.code : '',
        peers: game ? Object.keys(game.peers) : [],
        pings: pings,
        inDirectory: !!dir,
        dirPeers: dir ? Object.keys(dir.room.getPeers()) : [],
        dirEntries: dir ? Object.keys(dir.table).length : 0,
        announcing: dirAnnouncement,
        audio: ctx ? ctx.state : 'none',
        mic: micLive() ? (micFake ? 'fake' : 'live') : 'none',
        muted: muted,
        voices: Object.keys(voices),
        queued: { msgs: msgs.length, events: events.length, commands: commands.length },
        stats: stats,
        relays: relays
      };
    }, null)
  };

  window.TT = TT;

  // Trystero already says goodbye on beforeunload; pagehide covers the cases where that event is skipped
  // (mobile, some tab discards). A page that is only going into the back/forward cache stays connected.
  window.addEventListener('pagehide', function (e) {
    if (e && e.persisted) return;
    try { leaveGame(); } catch (err) { }
    try { dirLeave(); } catch (err) { }
  });

  window.ttCmd = function (str) {
    try {
      commands.push(String(str));
      if (commands.length > MAX_COMMANDS) commands.splice(0, commands.length - MAX_COMMANDS);
    } catch (e) { }
  };
})();
