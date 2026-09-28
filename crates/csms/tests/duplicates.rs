#![allow(clippy::unwrap_used)]

mod common;

use charger_sim::{ChargePoint, new_unique_id};
use common::{expected_cost, spawn};
use ocpp::{ErrorCode, Frame};
use serde_json::{Value, json};
use sqlx::PgPool;

fn start_payload() -> Value {
    json!({
        "connectorId": 1,
        "idTag": "DEMO-TAG-1",
        "meterStart": 1000,
        "timestamp": "2026-09-01T10:00:00Z"
    })
}

fn transaction_id(frame: &Frame) -> i32 {
    let Frame::CallResult { payload, .. } = frame else {
        panic!("expected CallResult, got {frame}")
    };
    payload["transactionId"].as_i64().unwrap() as i32
}

#[sqlx::test]
async fn repeated_start_transaction_returns_the_same_id_and_creates_one_session(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-DUP").await.unwrap();
    let uid = new_unique_id();

    let first = cp
        .call_raw(&uid, "StartTransaction", start_payload())
        .await
        .unwrap();
    let second = cp
        .call_raw(&uid, "StartTransaction", start_payload())
        .await
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(server.count("select count(*) from transactions").await, 1);
    let dups: i32 = sqlx::query_scalar("select duplicates from ocpp_messages where unique_id = $1")
        .bind(&uid)
        .fetch_one(&server.pool)
        .await
        .unwrap();
    assert_eq!(dups, 1);
}

#[sqlx::test]
async fn duplicate_survives_a_reconnect(pool: PgPool) {
    let server = spawn(pool).await;
    let uid = new_unique_id();

    let mut cp = ChargePoint::connect(&server.ocpp, "CP-DUP").await.unwrap();
    let first = cp
        .call_raw(&uid, "StartTransaction", start_payload())
        .await
        .unwrap();
    drop(cp);

    let mut cp = ChargePoint::connect(&server.ocpp, "CP-DUP").await.unwrap();
    let again = cp
        .call_raw(&uid, "StartTransaction", start_payload())
        .await
        .unwrap();

    assert_eq!(transaction_id(&first), transaction_id(&again));
    assert_eq!(server.count("select count(*) from transactions").await, 1);
}

#[sqlx::test]
async fn repeated_meter_values_are_stored_once(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-DUP").await.unwrap();
    let tx = transaction_id(
        &cp.call_raw(&new_unique_id(), "StartTransaction", start_payload())
            .await
            .unwrap(),
    );

    let uid = new_unique_id();
    let mv = json!({
        "connectorId": 1,
        "transactionId": tx,
        "meterValue": [{
            "timestamp": "2026-09-01T10:15:00Z",
            "sampledValue": [{"value": "2500", "measurand": "Energy.Active.Import.Register", "unit": "Wh"}]
        }]
    });
    for _ in 0..3 {
        cp.call_raw(&uid, "MeterValues", mv.clone()).await.unwrap();
    }

    assert_eq!(server.count("select count(*) from meter_values").await, 1);
    assert_eq!(server.session(tx).await["events"][1]["duplicates"], 2);
}

#[sqlx::test]
async fn repeated_stop_does_not_reprice(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-DUP").await.unwrap();
    let tx = transaction_id(
        &cp.call_raw(&new_unique_id(), "StartTransaction", start_payload())
            .await
            .unwrap(),
    );

    let stop = |meter_stop: i64| {
        json!({
            "transactionId": tx,
            "meterStop": meter_stop,
            "timestamp": "2026-09-01T11:00:00Z",
            "reason": "Local"
        })
    };
    let uid = new_unique_id();
    cp.call_raw(&uid, "StopTransaction", stop(21_000))
        .await
        .unwrap();
    cp.call_raw(&uid, "StopTransaction", stop(21_000))
        .await
        .unwrap();
    // A second stop under a new uniqueId (a charger that regenerated the id
    // on retry) must not overwrite the first either.
    cp.call_raw(&new_unique_id(), "StopTransaction", stop(99_000))
        .await
        .unwrap();

    let s = server.session(tx).await;
    assert_eq!(s["energyWh"], 20_000);
    assert_eq!(s["costMinor"], expected_cost(20_000));
}

#[sqlx::test]
async fn reused_unique_id_with_a_different_payload_is_a_protocol_error(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-DUP").await.unwrap();
    let uid = new_unique_id();

    cp.call_raw(&uid, "StartTransaction", start_payload())
        .await
        .unwrap();
    let mut other = start_payload();
    other["connectorId"] = json!(2);
    let reply = cp.call_raw(&uid, "StartTransaction", other).await.unwrap();

    let Frame::CallError(e) = reply else {
        panic!("expected CallError, got {reply}")
    };
    assert_eq!(e.code, ErrorCode::ProtocolError);
    assert_eq!(server.count("select count(*) from transactions").await, 1);
}

#[sqlx::test]
async fn unique_ids_are_scoped_per_charger(pool: PgPool) {
    let server = spawn(pool).await;
    let uid = "1".to_owned(); // chargers commonly use small counters
    let mut a = ChargePoint::connect(&server.ocpp, "CP-A").await.unwrap();
    let mut b = ChargePoint::connect(&server.ocpp, "CP-B").await.unwrap();

    let ta = transaction_id(
        &a.call_raw(&uid, "StartTransaction", start_payload())
            .await
            .unwrap(),
    );
    let tb = transaction_id(
        &b.call_raw(&uid, "StartTransaction", start_payload())
            .await
            .unwrap(),
    );
    assert_ne!(ta, tb);
}

#[sqlx::test]
async fn concurrent_copies_are_processed_once(pool: PgPool) {
    let server = spawn(pool).await;
    // Two sockets for the same charger, as when a charger reconnects before
    // the CSMS has noticed the old connection is dead.
    let mut a = ChargePoint::connect(&server.ocpp, "CP-RACE").await.unwrap();
    let mut b = ChargePoint::connect(&server.ocpp, "CP-RACE").await.unwrap();
    let uid = new_unique_id();

    let (ra, rb) = tokio::join!(
        a.call_raw(&uid, "StartTransaction", start_payload()),
        b.call_raw(&uid, "StartTransaction", start_payload()),
    );
    assert_eq!(transaction_id(&ra.unwrap()), transaction_id(&rb.unwrap()));
    assert_eq!(server.count("select count(*) from transactions").await, 1);
}

#[sqlx::test]
async fn malformed_payload_gets_a_formation_violation(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-BAD").await.unwrap();
    let reply = cp
        .call_raw(&new_unique_id(), "StartTransaction", json!({"idTag": 5}))
        .await
        .unwrap();
    let Frame::CallError(e) = reply else {
        panic!("expected CallError, got {reply}")
    };
    assert_eq!(e.code, ErrorCode::FormationViolation);

    let reply = cp
        .call_raw(&new_unique_id(), "DataTransfer", json!({"vendorId": "x"}))
        .await
        .unwrap();
    let Frame::CallError(e) = reply else {
        panic!("expected CallError, got {reply}")
    };
    assert_eq!(e.code, ErrorCode::NotImplemented);
}
