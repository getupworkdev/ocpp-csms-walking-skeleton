//! A minimal OCPP 1.6J charging station management system.

pub mod api;
pub mod gateway;
pub mod processor;
pub mod tariff;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use sqlx::PgPool;
use sqlx::migrate::Migrator;

use crate::gateway::Connections;
use crate::processor::Processor;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct AppState {
    pub processor: Processor,
    pub connections: Arc<Connections>,
}

impl AppState {
    pub fn new(pool: PgPool, heartbeat_interval_secs: i32) -> Self {
        Self {
            processor: Processor::new(pool, heartbeat_interval_secs),
            connections: Arc::default(),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/ocpp/{charger_id}", get(gateway::upgrade))
        .route("/api/chargers", get(api::list_chargers))
        .route("/api/chargers/{id}", get(api::get_charger))
        .route("/api/sessions", get(api::list_sessions))
        .route("/api/sessions/{transaction_id}", get(api::get_session))
        .route("/api/openapi.json", get(api::openapi_json))
        .route("/health", get(|| async { "ok" }))
        .with_state(state)
}
