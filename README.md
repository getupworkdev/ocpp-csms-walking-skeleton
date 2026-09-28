# ocpp-csms-walking-skeleton

[![ci](https://github.com/getupworkdev/ocpp-csms-walking-skeleton/actions/workflows/ci.yml/badge.svg)](https://github.com/getupworkdev/ocpp-csms-walking-skeleton/actions/workflows/ci.yml)

A thin, end-to-end slice of a charging station management system (CSMS) in Rust. It covers the
path from a charger's WebSocket to a priced session in Postgres, and handles the two ways real
chargers make that path messy: they resend messages, and they go offline and deliver the backlog
late.

Rust (tokio, axum, sqlx, serde, utoipa) · PostgreSQL 16 · OCPP 1.6J

## Architecture

```
 charger-sim ──── WebSocket, subprotocol ocpp1.6 ────┐
 (or a real        [2,"<uniqueId>","StartTransaction",{…}]
  charger)                                           │
                                          ┌──────────▼──────────┐
                                          │  gateway            │  GET /ocpp/{chargePointId}
                                          │  one task per socket│  calls handled in order
                                          └──────────┬──────────┘
                                                     │ (chargerId, uniqueId, action, payload)
                                          ┌──────────▼──────────┐
                                          │  processor          │  one DB transaction per call:
                                          │                     │   1. insert into ocpp_messages
                                          │   tariff (pure fn)  │      on conflict → replay stored answer
                                          └──────────┬──────────┘   2. handle, 3. store answer, commit
                                                     │
   REST  /api/chargers   ┌───────────────────────────▼───────────────────────────┐
         /api/sessions ──►  Postgres                                              │
   /api/openapi.json     │  chargers · id_tags · tariffs                          │
                         │  transactions   (serial id = OCPP transactionId)       │
                         │  ocpp_messages  (PK charger_id, unique_id; + response) │
                         │  meter_values   (every sample, by charger timestamp)   │
                         └────────────────────────────────────────────────────────┘
```

| Crate | What it is |
|---|---|
| [`crates/ocpp`](crates/ocpp) | OCPP-J framing (CALL / CALLRESULT / CALLERROR) and the six message payloads. No I/O. |
| [`crates/csms`](crates/csms) | The server: WebSocket gateway, message processor, tariff, REST API, migrations. |
| [`crates/charger-sim`](crates/charger-sim) | A scriptable charge point, used as a CLI and as the test driver. |

### Messages handled

| Action | What the CSMS does |
|---|---|
| BootNotification | Records vendor, model, serial and firmware; always `Accepted` with the configured heartbeat interval. |
| Heartbeat | Updates `last_heartbeat_at`. Not written to the message log: it has no side effects and never stops coming. |
| Authorize | Looks the tag up in `id_tags`. Unknown tags are `Invalid`, and past `expires_at` means `Expired`. |
| StartTransaction | **Allocates the transaction id** (Postgres serial) and snapshots the active tariff onto the session. A refused tag still gets an id, as 1.6 requires, and the status is recorded. |
| MeterValues | Stores every sampled value with the charger's timestamp. Informational only: it never affects the price. |
| StopTransaction | Prices the session from `meterStop - meterStart` and records the charger's stop time. A second stop for a finished session is acknowledged and ignored. |

Anything else gets `NotImplemented`. A payload that doesn't deserialise gets `FormationViolation`.

### Duplicates: processed once

The CALL is inserted into `ocpp_messages` with primary key `(charger_id, unique_id)` **inside the
same database transaction** that handles it and stores the response frame. So:

- A resend (same charger, same uniqueId, same payload) conflicts on insert and gets the stored
  response back, byte for byte. A duplicated StartTransaction returns the same transactionId.
  `duplicates` counts how often this happened.
- Two copies racing on two sockets serialise on the primary key. The second insert waits for the
  first transaction to commit, then conflicts.
- If handling fails (a database error), the transaction rolls back and nothing is stored, so the
  charger's retry is processed fresh rather than replaying an error.
- The same uniqueId with a *different* payload gets a `ProtocolError` instead of being silently
  swallowed. A charger whose id counter resets after reboot shows up in the logs.
- Ids are scoped per charger, because many chargers use small counters (`"1"`, `"2"`, …).

### Late and offline messages: priced correctly

The simulator behaves like a charger with an offline transaction queue. While the link is down,
MeterValues and StopTransaction are kept with their original uniqueId and timestamp and sent in
order on reconnect. It can also drop the socket right after sending, before the answer arrives.
That's the realistic source of duplicates: the CSMS processed it, the charger never found out, and
it sends it again.

The price can't be thrown off by any of that, because:

- **Energy is `meterStop - meterStart`** from the charger's own register, not a sum of MeterValues.
  Samples that arrive late, twice, out of order or not at all don't change it.
- **The tariff is snapshotted at StartTransaction**, so a price change while the charger is offline
  doesn't reprice the session.
- **Times are the charger's.** `stoppedAt` is when the charger stopped; `stopReceivedAt` is when we
  heard about it. `latestMeterWh` is the newest reading by sample time, not arrival time.
- Money is integer minor units, rounded half-up once at the end: 7.5 kWh at €0.35 plus a €0.50 fee
  is 313 cents.

## Running it

Needs Rust 1.88+ and a Postgres (16 is what CI uses).

```sh
createdb csms
DATABASE_URL=postgres://localhost/csms cargo run -p csms          # migrates, listens on :8180

# another terminal: a session that drops offline after 2 samples, loses the ack on
# the third, and replays the backlog 5s after it stopped
cargo run -p charger-sim -- --offline-after 2 --lose-ack --offline-secs 5

curl -s localhost:8180/api/sessions/1 | jq
```

```
transaction 1 stopped: 10000 Wh -> 17500 Wh (7500 Wh), 4 message(s) replayed after reconnect
```

`GET /api/sessions/{id}` returns the session with its cost, its event log (every OCPP message with
arrival time and duplicate count) and its meter samples. The migrations seed one tariff (EUR,
35c/kWh, 50c fee) and the tags `DEMO-TAG-1`, `DEMO-TAG-2` and `BLOCKED-TAG`.

Environment: `DATABASE_URL` (required), `BIND_ADDR` (default `0.0.0.0:8180`),
`HEARTBEAT_INTERVAL_SECS` (default 300), `RUST_LOG`.

### API

OpenAPI 3.1 is served at `/api/openapi.json` and committed as [`openapi.json`](openapi.json). A test
fails if the committed file drifts from the code; regenerate it with
`cargo run -p csms -- openapi > openapi.json`.

| | |
|---|---|
| `GET /api/chargers` | All chargers, with whether this instance holds a socket for each |
| `GET /api/chargers/{id}` | One charger |
| `GET /api/sessions?chargerId=&status=active\|completed&limit=` | Sessions, newest first |
| `GET /api/sessions/{transactionId}` | Session with cost, event log and meter samples |

## Tests

```sh
DATABASE_URL=postgres://localhost/postgres cargo test --workspace
```

`#[sqlx::test]` gives every test its own freshly migrated database. The integration tests start
the real router on a random port and drive it with the simulator over real WebSockets.

- [`tests/flow.rs`](crates/csms/tests/flow.rs): boot → authorize → start → 5 × MeterValues →
  stop, checked through the REST API (cost, energy, event order, charger state). Also covers the
  CSMS allocating ids across chargers, refused tags, rejecting sockets without `ocpp1.6`, and the
  OpenAPI document.
- [`tests/duplicates.rs`](crates/csms/tests/duplicates.rs): repeated StartTransaction (one
  session, same id), across a reconnect, repeated MeterValues (stored once), repeated and
  conflicting StopTransaction (no repricing), reused id with a different payload, per-charger id
  scope, and two copies racing on two sockets.
- [`tests/late.rs`](crates/csms/tests/late.rs): offline mid-session with a lost ack, with the
  backlog replayed after the session ended. Checks the exact cost, that the stop time is the
  charger's, one duplicate recorded and no sample stored twice. Also covers samples arriving
  *after* the stop, and a tariff change mid-session.

CI runs `cargo fmt --check`, clippy with `-D warnings`, and the tests against a Postgres 16
service. `clippy::unwrap_used` is denied for the whole workspace and allowed only in tests.

## What this proves / what it does not

**Proves**

- The core data path works end to end over a real socket: OCPP-J frames in, a priced session in
  Postgres, readable over a documented API.
- At-most-once *processing* on top of at-least-once *delivery*. Resends, reconnect replays and
  concurrent copies each have a test.
- A session's final cost is determined by what the charger measured, not by what order or how late
  the messages arrived.
- The CSMS, not the charger, owns transaction ids, and does so correctly under concurrency.
- The OpenAPI document is generated from the handlers and can't silently drift.

**Does not**

- **Security.** There's no TLS, no charger authentication (OCPP security profiles 1–3) and no auth
  on the REST API. Any client can claim any charge point id.
- **CSMS-initiated commands.** No RemoteStart/Stop, Reset, ChangeConfiguration, TriggerMessage or
  firmware updates. The gateway only answers calls.
- **A charger offline *at start*.** Real chargers start sessions offline and send StartTransaction
  late, then have to map their local session to the id they're given. The simulator only goes
  offline mid-session.
- **Most of OCPP 1.6.** There's no StatusNotification, DataTransfer, reservations, smart charging,
  local auth list sync, or connector state. There's no OCPP 2.0.1.
- **Real tariffs.** No time-of-use, idle or parking fees, taxes, currency conversion or roaming
  (OCPI). Prices are whole minor units per kWh.
- **Abandoned sessions.** A session whose StopTransaction never arrives stays active forever, and
  nothing closes it on a new StartTransaction for the same connector.
- **Scale or HA.** Connection state is in-process, so two instances would each think they own a
  charger. There's no message retention policy and no load testing.
- **Validation against real hardware** or the OCA test tooling (OCTT). "Works with the simulator I
  wrote" is weaker evidence than "works with three vendors' firmware".
