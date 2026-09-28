#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use charger_sim::{ChargePoint, Scenario, run};
use chrono::Utc;
use common::{expected_cost, spawn};
use ocpp::messages::AuthorizationStatus;
use sqlx::PgPool;
use utoipa::OpenApi;

#[sqlx::test]
async fn boot_start_meter_stop_produces_a_priced_session(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-FLOW").await.unwrap();

    let scenario = Scenario {
        samples: 5,
        wh_per_sample: 1_500,
        interval: Duration::from_millis(5),
        ..Scenario::default()
    };
    let report = run(&mut cp, &scenario).await.unwrap();
    assert_eq!(report.replayed, 0);

    let s = server.session(report.transaction_id).await;
    assert_eq!(s["status"], "completed");
    assert_eq!(s["chargerId"], "CP-FLOW");
    assert_eq!(s["meterStartWh"], 10_000);
    assert_eq!(s["meterStopWh"], 17_500);
    assert_eq!(s["energyWh"], 7_500);
    // 7.5 kWh * 0.35 = 2.625 -> 2.63, + 0.50 fee
    assert_eq!(s["costMinor"], expected_cost(7_500));
    assert_eq!(s["costMinor"], 313);
    assert_eq!(s["tariff"]["currency"], "EUR");
    assert_eq!(s["latestMeterWh"], 17_500);
    assert_eq!(s["stopReason"], "Local");

    let actions: Vec<&str> = s["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        actions,
        [
            "StartTransaction",
            "MeterValues",
            "MeterValues",
            "MeterValues",
            "MeterValues",
            "MeterValues",
            "StopTransaction"
        ]
    );
    assert_eq!(s["meterValues"].as_array().unwrap().len(), 5);

    let (status, charger) = server.get("/api/chargers/CP-FLOW").await;
    assert_eq!(status, 200);
    assert_eq!(charger["vendor"], "Skeleton");
    assert_eq!(charger["connected"], true);
    assert!(charger["lastBootAt"].is_string());

    let (_, sessions) = server
        .get("/api/sessions?chargerId=CP-FLOW&status=completed")
        .await;
    assert_eq!(sessions.as_array().unwrap().len(), 1);
    let (_, active) = server.get("/api/sessions?status=active").await;
    assert_eq!(active.as_array().unwrap().len(), 0);
}

#[sqlx::test]
async fn transaction_ids_are_assigned_by_the_csms(pool: PgPool) {
    let server = spawn(pool).await;
    let mut a = ChargePoint::connect(&server.ocpp, "CP-A").await.unwrap();
    let mut b = ChargePoint::connect(&server.ocpp, "CP-B").await.unwrap();

    let (t1, _) = a
        .start_transaction(1, "DEMO-TAG-1", 0, Utc::now())
        .await
        .unwrap();
    let (t2, _) = b
        .start_transaction(1, "DEMO-TAG-2", 0, Utc::now())
        .await
        .unwrap();
    let (t3, _) = a
        .start_transaction(2, "DEMO-TAG-2", 0, Utc::now())
        .await
        .unwrap();

    assert!(t1 < t2 && t2 < t3, "{t1} {t2} {t3}");
    assert_eq!(server.session(t2).await["chargerId"], "CP-B");
}

#[sqlx::test]
async fn unknown_and_blocked_tags_are_refused_but_still_get_an_id(pool: PgPool) {
    let server = spawn(pool).await;
    let mut cp = ChargePoint::connect(&server.ocpp, "CP-TAGS").await.unwrap();

    assert_eq!(
        cp.authorize("NOBODY").await.unwrap().status,
        AuthorizationStatus::Invalid
    );
    let (id, info) = cp
        .start_transaction(1, "BLOCKED-TAG", 0, Utc::now())
        .await
        .unwrap();
    assert_eq!(info.status, AuthorizationStatus::Blocked);
    assert_eq!(server.session(id).await["idTagStatus"], "Blocked");
}

#[sqlx::test]
async fn websocket_without_ocpp_subprotocol_is_rejected(pool: PgPool) {
    use tokio_tungstenite::tungstenite::Error;

    let server = spawn(pool).await;
    let err = tokio_tungstenite::connect_async(format!("{}/CP-X", server.ocpp))
        .await
        .unwrap_err();
    let Error::Http(res) = err else {
        panic!("expected an HTTP rejection, got {err:?}")
    };
    assert_eq!(res.status(), 400);
}

#[sqlx::test]
async fn unknown_session_is_404(pool: PgPool) {
    let server = spawn(pool).await;
    let (status, body) = server.get("/api/sessions/999").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"], "session not found");
}

#[sqlx::test]
async fn openapi_document_is_served_as_3_1(pool: PgPool) {
    let server = spawn(pool).await;
    let (status, doc) = server.get("/api/openapi.json").await;
    assert_eq!(status, 200);
    assert_eq!(doc["openapi"], "3.1.0");
    for path in [
        "/api/chargers",
        "/api/chargers/{id}",
        "/api/sessions",
        "/api/sessions/{transactionId}",
    ] {
        assert!(doc["paths"][path].is_object(), "missing {path}");
    }
}

#[test]
fn committed_openapi_json_is_current() {
    let generated = csms::api::ApiDoc::openapi().to_pretty_json().unwrap();
    let committed = include_str!("../../../openapi.json");
    assert_eq!(
        committed.trim(),
        generated.trim(),
        "openapi.json is stale: run `cargo run -p csms -- openapi > openapi.json`"
    );
}
