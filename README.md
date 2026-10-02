# Flock Gateway

The store-and-forward core of **Reinnder Flock**, a poultry house monitoring kit in prototype
([Reinnder Technologies](https://www.reinndertechnologies.com/flock)).

It sits between sensor nodes in a poultry house and a cloud service. It keeps every reading, in order,
through an internet outage and a restart of the program, and it raises simple local alerts with no internet at all.

## Status: a prototype, tested on a bench only

- **Tested:** in software, on a bench, against a stand-in for the cloud. See `test.mjs`.
- **Not tested:** in a working poultry house, on real hardware, or through a real power cut.
  The test restarts the program; it does not cut the power. Readings are written with ordinary file
  appends (not forced to disk), so a power cut at the wrong moment could lose the most recent readings.
- **Not included:** the sensor-node firmware (it has not yet run on hardware) and any cloud service.
- **Not a safety system.** Do not make it the only alarm for animal welfare or building control.
  Flock is meant to sit beside a farm's own controllers as a second pair of eyes. It does not run fans, heaters or feeders.
- **No authentication.** Anyone who can reach its port can send it readings. Run it on a trusted local network only.

## What it does

- Receives readings from nodes: `POST /ingest` with JSON.
- Stamps each reading with the gateway's own clock and the next number in an unbroken sequence (`seq`).
- Writes it to a queue file on disk **before** replying to the node.
- Checks it against heat, cold and humidity limits, with a small margin (hysteresis) so an alarm does not flicker at the limit.
- Sends readings to the cloud in order, in batches, **at least once**. It drops a reading from the queue only after the cloud has acknowledged it.
- Picks up where it left off after a restart.
- Serves `GET /status` (JSON) and a small maintenance page at `GET /`.

No dependencies: only Node.js built-ins.

## Run it

Requires Node.js 20 or newer (tested on Node 24).

```bash
node --test test.mjs
```

```bash
PORT=8791 CLOUD_URL=http://127.0.0.1:8792/sync node server.mjs
curl -X POST localhost:8791/ingest -H 'content-type: application/json' -d '{"device_id":"node-01","temp_c":27.4,"rh":61}'
curl localhost:8791/status
```

Settings (environment variables): `PORT` (default 8791), `CLOUD_URL` (default `http://127.0.0.1:8792/sync`),
`DATA_DIR` (default `./data`), `GATEWAY_ID` (default `gw-dev-001`).

The alert limits are options in `createGateway` (`tempHigh` 32, `tempLow` 18, `rhHigh` 85, `hysteresis` 1).
They are examples for the tests, not advice for any flock.

## Message format (schema 1)

Node to gateway: `device_id` (required), and optionally `farm_id`, `house_id`, `fw`, `temp_c`, `rh`, `sound_db`,
`battery_v`, `sharp`, `uptime_s`. `temp_c`, `rh`, `sound_db` and `battery_v` must be numbers.
`sound_db` is a relative level, not calibrated. The gateway adds `seq` (increases by one and survives restarts),
`gw_id`, `gw_ts` (ISO time) and `schema`. A node cannot override `seq` or `gw_id`.

Gateway to cloud: `POST` to `CLOUD_URL` with `{ "gateway_id": "...", "items": [ ... ] }`, and the cloud answers
`{ "acked_seq": N }`. Delivery is at least once, so the cloud should keep one copy of each reading by `gateway_id` plus `seq`.

## Contributing

Issues and pull requests are welcome. This is an early prototype, so expect it to change.

## Licence

MIT. See [LICENSE](LICENSE).

## More

- The story behind it: [How a poultry house gateway keeps your data when the internet drops](https://www.reinndertechnologies.com/newsroom/poultry-house-gateway-keeps-your-data)
- Reinnder Flock: https://www.reinndertechnologies.com/flock
