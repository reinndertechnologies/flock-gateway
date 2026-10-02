// Proves the V1 core loop: outage -> buffering -> restart -> reconnection -> ordered synchronisation.
// Run: node --test prototype/gateway/test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createGateway } from './server.mjs';

function makeCloud() {
  const received = []; const seen = new Set(); let up = true; let server;
  const api = {
    received,
    setUp: (v) => { up = v; },
    start: () => new Promise((resolve) => {
      server = http.createServer((req, res) => {
        if (!up) { res.writeHead(503); return res.end('down'); }
        const chunks = [];
        req.on('data', (c) => chunks.push(c));
        req.on('end', () => {
          const { gateway_id, items } = JSON.parse(Buffer.concat(chunks));
          let max = 0;
          for (const it of items) {
            const key = gateway_id + ':' + it.seq;
            if (!seen.has(key)) { seen.add(key); received.push(it); }
            max = Math.max(max, it.seq);
          }
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ acked_seq: max }));
        });
      }).listen(0, () => resolve(server.address().port));
    }),
    stop: () => new Promise((r) => { server.close(() => r()); server.closeAllConnections?.(); }),
  };
  return api;
}

const post = (port, body) => fetch(`http://127.0.0.1:${port}/ingest`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
const getStatus = async (port) => (await fetch(`http://127.0.0.1:${port}/status`)).json();
async function waitFor(fn, ms = 8000) {
  const t = Date.now();
  while (Date.now() - t < ms) { if (await fn()) return; await new Promise((r) => setTimeout(r, 50)); }
  throw new Error('timed out waiting');
}
const reading = (i, temp = 27) => ({ device_id: 'node-01', farm_id: 'f1', house_id: 'h1', fw: '0.1.0', temp_c: temp, rh: 60 + (i % 5), sound_db: 40 });

test('outage -> buffer -> gateway restart -> reconnect -> ordered sync with no duplicates', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-gw-'));
  const cloud = makeCloud();
  const cloudPort = await cloud.start();
  const mk = () => createGateway({ port: 0, dataDir, cloudUrl: `http://127.0.0.1:${cloudPort}/sync`, syncMs: 100, onAlert: () => {} });

  let gw = mk(); let port = await gw.start();
  try {
  for (let i = 1; i <= 5; i++) assert.equal((await post(port, reading(i))).status, 200);
  await waitFor(() => cloud.received.length === 5);
  assert.deepEqual(cloud.received.map((x) => x.seq), [1, 2, 3, 4, 5]);

  cloud.setUp(false);                               // internet goes away
  for (let i = 6; i <= 15; i++) await post(port, reading(i, i >= 12 ? 34 : 27));
  await waitFor(async () => (await getStatus(port)).cloud === 'offline');
  const during = await getStatus(port);
  assert.equal(during.queue_depth, 10);             // buffered, nothing lost
  assert.equal(during.alarm, true);                 // local safety rule fired with no cloud
  assert.equal(cloud.received.length, 5);

  await gw.stop();                                  // power cycle the gateway mid-outage
  gw = mk(); port = await gw.start();
  assert.equal((await getStatus(port)).queue_depth, 10);   // queue survived the restart
  await post(port, reading(16));                    // new reading gets the next sequence number
  assert.equal((await getStatus(port)).last_seq, 16);

  cloud.setUp(true);                                // internet returns
  await waitFor(() => cloud.received.length === 16);
  assert.deepEqual(cloud.received.map((x) => x.seq), Array.from({ length: 16 }, (_, i) => i + 1));
  const after = await getStatus(port);
  assert.equal(after.queue_depth, 0);
  assert.equal(after.cloud, 'online');
  for (let i = 1; i < cloud.received.length; i++)   // gateway clock never goes backwards
    assert.ok(cloud.received[i].gw_ts >= cloud.received[i - 1].gw_ts);

  } finally { await gw.stop(); await cloud.stop(); }
});

test('local alert clears with hysteresis and rejects bad input', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-gw-'));
  const events = [];
  const gw = createGateway({ port: 0, dataDir, cloudUrl: 'http://127.0.0.1:1/sync', syncMs: 60000, onAlert: (e) => events.push(e) });
  const port = await gw.start();
  await post(port, reading(1, 33)); await post(port, reading(2, 34));
  assert.equal(events.length, 1);                              // one alert, not one per reading
  assert.equal((await getStatus(port)).alarm, true);
  await post(port, reading(3, 31.5));                          // inside hysteresis: still active
  assert.equal((await getStatus(port)).alarm, true);
  await post(port, reading(4, 29));                            // clearly normal: clears
  assert.equal((await getStatus(port)).alarm, false);
  assert.equal((await post(port, { temp_c: 20 })).status, 400);                      // no device_id
  assert.equal((await post(port, { device_id: 'n', temp_c: 'hot' })).status, 400);   // not a number
  await gw.stop();
});

test('the maintenance page escapes what a node sends', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-gw-'));
  const gw = createGateway({ port: 0, dataDir, cloudUrl: 'http://127.0.0.1:1/sync', syncMs: 60000, onAlert: () => {} });
  const port = await gw.start();
  try {
    assert.equal((await post(port, { device_id: '<script>alert(1)</script>"x"', temp_c: 25 })).status, 200);
    const page = await (await fetch(`http://127.0.0.1:${port}/`)).text();
    assert.ok(!page.includes('<script>'), 'no markup from a node reaches the page');
    assert.ok(page.includes('&lt;script&gt;alert(1)&lt;/script&gt;'), 'it is shown as text instead');
  } finally { await gw.stop(); }
});
