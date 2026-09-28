#![allow(dead_code, clippy::unwrap_used)]

use csms::{AppState, router};
use serde_json::Value;
use sqlx::PgPool;

pub struct Server {
    pub http: String,
    pub ocpp: String,
    pub pool: PgPool,
}

/// Run the real router on an ephemeral port against the test database.
pub async fn spawn(pool: PgPool) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState::new(pool.clone(), 60));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        http: format!("http://{addr}"),
        ocpp: format!("ws://{addr}/ocpp"),
        pool,
    }
}

impl Server {
    pub async fn get(&self, path: &str) -> (u16, Value) {
        let res = reqwest::get(format!("{}{path}", self.http)).await.unwrap();
        let status = res.status().as_u16();
        (status, res.json().await.unwrap())
    }

    pub async fn session(&self, transaction_id: i32) -> Value {
        let (status, body) = self.get(&format!("/api/sessions/{transaction_id}")).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    pub async fn count(&self, sql: &'static str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(&self.pool).await.unwrap()
    }
}

/// Fee 50 + 35/kWh, rounded half up: the seeded tariff.
pub fn expected_cost(energy_wh: i64) -> i64 {
    50 + (energy_wh * 35 + 500) / 1000
}
