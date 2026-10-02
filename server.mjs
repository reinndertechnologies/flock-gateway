// Reinnder Flock Gateway, store-and-forward core.
// Receives node readings, stamps them with a gateway clock and a sequence number, queues them on disk,
// runs local threshold rules, and syncs to the cloud in order. Survives cloud outages and restarts.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const NUMERIC = ['temp_c', 'rh', 'sound_db', 'battery_v'];

export function createGateway(opts = {}) {
  const cfg = {
    port: 8791,
    dataDir: path.join(here, 'data'),
    cloudUrl: 'http://127.0.0.1:8792/sync',
    gatewayId: 'gw-dev-001',
    tempHigh: 32, tempLow: 18, rhHigh: 85, hysteresis: 1,
    syncMs: 1000, batch: 50, cloudTimeoutMs: 3000,
    onAlert: (e) => console.log('[alert]', JSON.stringify(e)),
    ...opts,
  };
  fs.mkdirSync(cfg.dataDir, { recursive: true });
  const queueFile = path.join(cfg.dataDir, 'queue.jsonl');
  const stateFile = path.join(cfg.dataDir, 'state.json');

  // ---- durable state ----
  let state = { seq: 0, acked: 0 };
  try { state = { ...state, ...JSON.parse(fs.readFileSync(stateFile, 'utf8')) }; } catch { /* first run */ }
  let pending = [];
  try {
    pending = fs.readFileSync(queueFile, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
  } catch { /* no queue yet */ }
  if (pending.length) state.seq = Math.max(state.seq, pending[pending.length - 1].seq);
  pending = pending.filter((i) => i.seq > state.acked);

  const saveState = () => fs.writeFileSync(stateFile, JSON.stringify(state));
  const compact = () => {
    const tmp = queueFile + '.tmp';
    fs.writeFileSync(tmp, pending.map((i) => JSON.stringify(i) + '\n').join(''));
    fs.renameSync(tmp, queueFile);
    saveState();
  };

  // ---- local rules (work with no internet) ----
  const alerts = new Map(); // key -> { type, device_id, since, last, value, active }
  const last = new Map();   // device_id -> latest item
  function evaluate(item) {
    const checks = [
      ['heat_high', item.temp_c, (v) => v >= cfg.tempHigh, (v) => v < cfg.tempHigh - cfg.hysteresis],
      ['cold_low', item.temp_c, (v) => v <= cfg.tempLow, (v) => v > cfg.tempLow + cfg.hysteresis],
      ['humidity_high', item.rh, (v) => v >= cfg.rhHigh, (v) => v < cfg.rhHigh - cfg.hysteresis],
    ];
    for (const [type, value, trips, clears] of checks) {
      if (typeof value !== 'number') continue;
      const key = item.device_id + ':' + type;
      const a = alerts.get(key);
      if (trips(value)) {
        if (!a || !a.active) {
          const e = { type, device_id: item.device_id, house_id: item.house_id, value, gw_ts: item.gw_ts, seq: item.seq };
          alerts.set(key, { ...e, since: item.gw_ts, last: item.gw_ts, active: true });
          cfg.onAlert(e);
        } else { a.last = item.gw_ts; a.value = value; }
      } else if (a && a.active && clears(value)) a.active = false;
    }
  }

  // ---- cloud sync, in order, at-least-once (cloud dedupes on gateway_id + seq) ----
  let cloud = 'unknown', lastError = null, lastSyncAt = null, syncing = false, timer = null;
  async function syncOnce() {
    if (!pending.length) return false;
    const batch = pending.slice(0, cfg.batch);
    try {
      const res = await fetch(cfg.cloudUrl, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ gateway_id: cfg.gatewayId, items: batch }),
        signal: AbortSignal.timeout(cfg.cloudTimeoutMs),
      });
      if (!res.ok) throw new Error('cloud HTTP ' + res.status);
      const { acked_seq } = await res.json();
      if (!(acked_seq > state.acked)) throw new Error('cloud did not advance ack');
      state.acked = acked_seq;
      pending = pending.filter((i) => i.seq > state.acked);
      compact();
      cloud = 'online'; lastError = null; lastSyncAt = new Date().toISOString();
      return pending.length > 0;
    } catch (e) {
      cloud = 'offline'; lastError = String(e.message || e);
      return false;
    }
  }
  async function drain() {
    if (syncing) return;
    syncing = true;
    try { while (await syncOnce()); } finally { syncing = false; }
  }

  // ---- HTTP: node ingest and local maintenance view ----
  function status() {
    return {
      gateway_id: cfg.gatewayId,
      cloud, last_error: lastError, last_sync_at: lastSyncAt,
      queue_depth: pending.length, last_seq: state.seq, acked_seq: state.acked,
      alarm: [...alerts.values()].some((a) => a.active),
      alerts: [...alerts.values()],
      devices: [...last.values()],
    };
  }
  function readJson(req, limit = 65536) {
    return new Promise((resolve, reject) => {
      let size = 0; const chunks = [];
      req.on('data', (c) => { size += c.length; if (size > limit) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
      req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch { reject(new Error('bad json')); } });
      req.on('error', reject);
    });
  }
  // Anything a node sends is printed on the maintenance page, so it is escaped first.
  const esc = (v) => String(v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const send = (res, code, body, type = 'application/json') => { res.writeHead(code, { 'content-type': type }); res.end(typeof body === 'string' ? body : JSON.stringify(body)); };

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    if (req.method === 'POST' && url.pathname === '/ingest') {
      let body;
      try { body = await readJson(req); } catch (e) { return send(res, 400, { error: e.message }); }
      if (!body || typeof body.device_id !== 'string' || !body.device_id) return send(res, 400, { error: 'device_id required' });
      for (const k of NUMERIC) if (k in body && !Number.isFinite(body[k])) return send(res, 400, { error: k + ' must be a number' });
      const item = { seq: ++state.seq, gw_id: cfg.gatewayId, gw_ts: new Date().toISOString(), schema: 1, ...body };
      item.seq = state.seq; item.gw_id = cfg.gatewayId; // clients cannot override these
      fs.appendFileSync(queueFile, JSON.stringify(item) + '\n');
      pending.push(item); last.set(item.device_id, item);
      evaluate(item);
      send(res, 200, { seq: item.seq });
      drain();
    } else if (req.method === 'GET' && url.pathname === '/status') {
      send(res, 200, status());
    } else if (req.method === 'GET' && url.pathname === '/') {
      const s = status();
      send(res, 200, `<!doctype html><meta charset="utf-8"><title>Flock Gateway</title><body style="font:16px system-ui;max-width:640px;margin:2rem auto;padding:0 1rem"><h1>Flock Gateway</h1><p>Cloud: <b>${esc(s.cloud)}</b> · Queue: <b>${esc(s.queue_depth)}</b> · Alarm: <b>${s.alarm ? 'ON' : 'off'}</b></p><pre>${esc(JSON.stringify(s.devices, null, 2))}</pre>`, 'text/html');
    } else send(res, 404, { error: 'not found' });
  });

  return {
    cfg,
    status,
    start: () => new Promise((resolve) => {
      server.listen(cfg.port, () => { timer = setInterval(drain, cfg.syncMs); resolve(server.address().port); });
    }),
    stop: () => new Promise((resolve) => {
      clearInterval(timer);
      server.close(() => resolve());
      server.closeAllConnections?.();
    }),
    drain,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const o = {};
  if (process.env.PORT) o.port = Number(process.env.PORT);
  if (process.env.CLOUD_URL) o.cloudUrl = process.env.CLOUD_URL;
  if (process.env.DATA_DIR) o.dataDir = process.env.DATA_DIR;
  if (process.env.GATEWAY_ID) o.gatewayId = process.env.GATEWAY_ID;
  createGateway(o).start().then((p) => console.log('Flock gateway listening on http://localhost:' + p));
}
