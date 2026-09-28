#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use charger_sim::{ChargePoint, Delivery, Offline, Scenario, run};
use chrono::{DateTime, TimeDelta, Utc};
use common::{expected_cost, spawn};
use sqlx::PgPool;

#[sqlx::test]
async fn offline_mid_session_replays_late_and_prices_correctly(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-LATE").await.unwrap();

    // 6 samples; link drops after 2. Sample 3 reaches the CSMS but its answer
    // is lost, so it is queued too. Samples 4-6 and the stop queue while
    // offline and all go out a second after the session actually ended.
    let scenario = Scenario {
        samples: 6,
        wh_per_sample: 2_000,
        interval: Duration::from_millis(5),
        offline: Some(Offline {
            after_samples: 2,
            for_duration: Duration::from_secs(1),
            lose_ack: true,
        }),
        ..Scenario::default()
    };
    let report = run(&mut cp, &scenario).await.unwrap();
    assert_eq!(report.replayed, 5, "sample 3 again, samples 4-6, stop");
    assert_eq!(cp.queued(), 0);

    let s = server.session(report.transaction_id).await;
    assert_eq!(s["status"], "completed");
    assert_eq!(s["energyWh"], 12_000);
    assert_eq!(s["costMinor"], expected_cost(12_000));

    // stoppedAt is the charger's clock, not when the late stop arrived.
    let stopped_at: DateTime<Utc> = serde_json::from_value(s["stoppedAt"].clone()).unwrap();
    let received: DateTime<Utc> = serde_json::from_value(s["stopReceivedAt"].clone()).unwrap();
    assert!((stopped_at - report.stopped_at).abs() < TimeDelta::milliseconds(1));
    assert!(
        received - stopped_at >= TimeDelta::milliseconds(900),
        "{stopped_at} {received}"
    );

    // The message whose ack was lost was processed once and seen twice.
    let dups: i64 = sqlx::query_scalar("select sum(duplicates)::bigint from ocpp_messages")
        .fetch_one(&server.pool)
        .await
        .unwrap();
    assert_eq!(dups, 1);
    let samples = s["meterValues"].as_array().unwrap();
    assert_eq!(samples.len(), 6, "no sample stored twice");
    let registers: Vec<i64> = samples
        .iter()
        .map(|m| m["energyWh"].as_i64().unwrap())
        .collect();
    assert_eq!(registers, [12_000, 14_000, 16_000, 18_000, 20_000, 22_000]);
}

#[sqlx::test]
async fn meter_values_arriving_after_the_stop_do_not_change_the_cost(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-ORDER")
        .await
        .unwrap();
    let t0 = Utc::now() - TimeDelta::hours(2);

    let (tx, _) = cp
        .start_transaction(1, "DEMO-TAG-1", 5_000, t0)
        .await
        .unwrap();
    // The stop reaches the CSMS first...
    let stop = cp
        .stop_transaction(tx, 15_000, t0 + TimeDelta::minutes(60), "EVDisconnected")
        .await
        .unwrap();
    assert_eq!(stop, Delivery::Sent);
    let priced = server.session(tx).await;

    // ...then samples from during the session straggle in from another queue,
    // including one reading higher than meterStop.
    for (mins, wh) in [(20, 8_000), (40, 11_000), (59, 16_000)] {
        cp.meter_values(1, tx, wh, t0 + TimeDelta::minutes(mins))
            .await
            .unwrap();
    }

    let after = server.session(tx).await;
    assert_eq!(after["energyWh"], 10_000);
    assert_eq!(after["costMinor"], expected_cost(10_000));
    assert_eq!(after["costMinor"], priced["costMinor"]);
    assert_eq!(after["stoppedAt"], priced["stoppedAt"]);
    assert_eq!(after["meterValues"].as_array().unwrap().len(), 3);
}

#[sqlx::test]
async fn tariff_change_mid_session_does_not_reprice_it(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-TARIFF")
        .await
        .unwrap();
    let t0 = Utc::now() - TimeDelta::hours(1);

    let (tx, _) = cp.start_transaction(1, "DEMO-TAG-1", 0, t0).await.unwrap();
    sqlx::query("update tariffs set active = false")
        .execute(&server.pool)
        .await
        .unwrap();
    sqlx::query(
        "insert into tariffs (name, currency, price_per_kwh_minor, session_fee_minor, active)
         values ('Peak', 'EUR', 90, 100, true)",
    )
    .execute(&server.pool)
    .await
    .unwrap();

    cp.stop_transaction(tx, 10_000, t0 + TimeDelta::minutes(30), "Local")
        .await
        .unwrap();
    assert_eq!(server.session(tx).await["costMinor"], expected_cost(10_000));
}
