//! Read-only REST API over chargers and sessions, documented with utoipa.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::AppState;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "OCPP CSMS walking skeleton",
        description = "Chargers and charging sessions recorded by the OCPP 1.6J gateway. \
                       Charge points themselves connect over WebSocket at `/ocpp/{chargePointId}`, \
                       which is not part of this HTTP API."
    ),
    paths(list_chargers, get_charger, list_sessions, get_session),
    tags(
        (name = "chargers", description = "Charge points that have connected"),
        (name = "sessions", description = "Charging sessions (OCPP transactions)")
    )
)]
pub struct ApiDoc;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    pub error: String,
}

pub enum Error {
    NotFound(&'static str),
    Db(sqlx::Error),
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::NotFound(what) => (StatusCode::NOT_FOUND, format!("{what} not found")),
            Self::Db(e) => {
                tracing::error!(error = %e, "api query failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_owned(),
                )
            }
        };
        (status, Json(ApiError { error })).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Charger {
    /// Charge point identity from the WebSocket URL.
    pub id: String,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial_number: Option<String>,
    pub firmware_version: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub last_boot_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// Whether this CSMS instance currently holds a WebSocket for it.
    #[sqlx(skip)]
    pub connected: bool,
}

// Macros rather than consts so concat! yields the &'static str sqlx wants for
// statically known SQL.
macro_rules! charger_select {
    () => {
        "select id, vendor, model, serial_number, firmware_version, first_seen_at, \
         last_seen_at, last_boot_at, last_heartbeat_at from chargers"
    };
}

#[utoipa::path(
    get,
    path = "/api/chargers",
    tag = "chargers",
    responses((status = 200, body = Vec<Charger>))
)]
pub async fn list_chargers(State(state): State<AppState>) -> Result<Json<Vec<Charger>>, Error> {
    let mut chargers: Vec<Charger> = sqlx::query_as(concat!(charger_select!(), " order by id"))
        .fetch_all(state.processor.pool())
        .await?;
    for c in &mut chargers {
        c.connected = state.connections.is_connected(&c.id);
    }
    Ok(Json(chargers))
}

#[utoipa::path(
    get,
    path = "/api/chargers/{id}",
    tag = "chargers",
    params(("id" = String, Path, description = "Charge point identity")),
    responses(
        (status = 200, body = Charger),
        (status = 404, body = ApiError)
    )
)]
pub async fn get_charger(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Charger>, Error> {
    let mut charger: Charger = sqlx::query_as(concat!(charger_select!(), " where id = $1"))
        .bind(&id)
        .fetch_optional(state.processor.pool())
        .await?
        .ok_or(Error::NotFound("charger"))?;
    charger.connected = state.connections.is_connected(&charger.id);
    Ok(Json(charger))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    Active,
    Completed,
}

/// A charging session. Times are the charger's own timestamps; `*ReceivedAt`
/// are when the CSMS got the message, which differ when a charger was offline.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    /// OCPP transactionId, assigned by the CSMS.
    pub transaction_id: i32,
    pub charger_id: String,
    pub connector_id: i32,
    pub id_tag: String,
    pub id_tag_status: String,
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub stop_reason: Option<String>,
    pub meter_start_wh: i64,
    pub meter_stop_wh: Option<i64>,
    /// Most recent energy register reading by sample time (not arrival time).
    pub latest_meter_wh: Option<i64>,
    /// meterStop - meterStart, set when the session stops.
    pub energy_wh: Option<i64>,
    pub tariff: TariffSnapshot,
    /// Final price in minor units of `tariff.currency`, set when the session stops.
    pub cost_minor: Option<i64>,
    pub start_received_at: DateTime<Utc>,
    pub stop_received_at: Option<DateTime<Utc>>,
}

/// The tariff as it was when the session started.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TariffSnapshot {
    pub tariff_id: i32,
    pub currency: String,
    pub price_per_kwh_minor: i64,
    pub session_fee_minor: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionDetail {
    #[serde(flatten)]
    pub session: Session,
    /// Every OCPP message logged against this transaction, in arrival order.
    pub events: Vec<SessionEvent>,
    /// Energy register samples in sample-time order.
    pub meter_values: Vec<MeterSample>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct SessionEvent {
    pub unique_id: String,
    pub action: String,
    pub received_at: DateTime<Utc>,
    /// How many further copies of this message were received and not reprocessed.
    pub duplicates: i32,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct MeterSample {
    pub sampled_at: DateTime<Utc>,
    pub energy_wh: i64,
    pub source_unique_id: String,
}

#[derive(sqlx::FromRow)]
struct SessionRow {
    id: i32,
    charger_id: String,
    connector_id: i32,
    id_tag: String,
    id_tag_status: String,
    started_at: DateTime<Utc>,
    stopped_at: Option<DateTime<Utc>>,
    stop_reason: Option<String>,
    meter_start_wh: i64,
    meter_stop_wh: Option<i64>,
    latest_meter_wh: Option<i64>,
    energy_wh: Option<i64>,
    tariff_id: i32,
    currency: String,
    price_per_kwh_minor: i64,
    session_fee_minor: i64,
    cost_minor: Option<i64>,
    start_received_at: DateTime<Utc>,
    stop_received_at: Option<DateTime<Utc>>,
}

impl From<SessionRow> for Session {
    fn from(r: SessionRow) -> Self {
        Self {
            transaction_id: r.id,
            charger_id: r.charger_id,
            connector_id: r.connector_id,
            id_tag: r.id_tag,
            id_tag_status: r.id_tag_status,
            status: if r.stopped_at.is_some() {
                SessionStatus::Completed
            } else {
                SessionStatus::Active
            },
            started_at: r.started_at,
            stopped_at: r.stopped_at,
            stop_reason: r.stop_reason,
            meter_start_wh: r.meter_start_wh,
            meter_stop_wh: r.meter_stop_wh,
            latest_meter_wh: r.latest_meter_wh,
            energy_wh: r.energy_wh,
            tariff: TariffSnapshot {
                tariff_id: r.tariff_id,
                currency: r.currency,
                price_per_kwh_minor: r.price_per_kwh_minor,
                session_fee_minor: r.session_fee_minor,
            },
            cost_minor: r.cost_minor,
            start_received_at: r.start_received_at,
            stop_received_at: r.stop_received_at,
        }
    }
}

macro_rules! session_select {
    () => {
        "
    select t.id, t.charger_id, t.connector_id, t.id_tag, t.id_tag_status, t.started_at,
           t.stopped_at, t.stop_reason, t.meter_start_wh, t.meter_stop_wh,
           (select mv.energy_wh from meter_values mv
             where mv.transaction_id = t.id and mv.charger_id = t.charger_id
               and mv.energy_wh is not null
             order by mv.sampled_at desc limit 1) as latest_meter_wh,
           t.energy_wh, t.tariff_id, t.currency::text as currency, t.price_per_kwh_minor,
           t.session_fee_minor, t.cost_minor, t.start_received_at, t.stop_received_at
    from transactions t"
    };
}

#[derive(Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct SessionFilter {
    /// Only sessions on this charger.
    pub charger_id: Option<String>,
    pub status: Option<SessionStatus>,
    /// Page size, 1 to 500. Defaults to 50.
    pub limit: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/sessions",
    tag = "sessions",
    params(SessionFilter),
    responses((status = 200, body = Vec<Session>, description = "Newest first"))
)]
pub async fn list_sessions(
    State(state): State<AppState>,
    Query(filter): Query<SessionFilter>,
) -> Result<Json<Vec<Session>>, Error> {
    let rows: Vec<SessionRow> = sqlx::query_as(concat!(
        session_select!(),
        "
         where ($1::text is null or t.charger_id = $1)
           and ($2::text is null
                or ($2 = 'active' and t.stopped_at is null)
                or ($2 = 'completed' and t.stopped_at is not null))
         order by t.started_at desc, t.id desc
         limit $3"
    ))
    .bind(filter.charger_id)
    .bind(filter.status.map(|s| match s {
        SessionStatus::Active => "active",
        SessionStatus::Completed => "completed",
    }))
    .bind(filter.limit.unwrap_or(50).clamp(1, 500))
    .fetch_all(state.processor.pool())
    .await?;
    Ok(Json(rows.into_iter().map(Session::from).collect()))
}

#[utoipa::path(
    get,
    path = "/api/sessions/{transactionId}",
    tag = "sessions",
    params(("transactionId" = i32, Path, description = "OCPP transactionId")),
    responses(
        (status = 200, body = SessionDetail),
        (status = 404, body = ApiError)
    )
)]
pub async fn get_session(
    State(state): State<AppState>,
    Path(transaction_id): Path<i32>,
) -> Result<Json<SessionDetail>, Error> {
    let pool = state.processor.pool();
    let row: SessionRow = sqlx::query_as(concat!(session_select!(), " where t.id = $1"))
        .bind(transaction_id)
        .fetch_optional(pool)
        .await?
        .ok_or(Error::NotFound("session"))?;

    let events: Vec<SessionEvent> = sqlx::query_as(
        "select unique_id, action, received_at, duplicates from ocpp_messages
         where transaction_id = $1 and charger_id = $2
         order by received_at, unique_id",
    )
    .bind(transaction_id)
    .bind(&row.charger_id)
    .fetch_all(pool)
    .await?;

    let meter_values: Vec<MeterSample> = sqlx::query_as(
        "select sampled_at, energy_wh, source_unique_id from meter_values
         where transaction_id = $1 and charger_id = $2 and energy_wh is not null
         order by sampled_at, id",
    )
    .bind(transaction_id)
    .bind(&row.charger_id)
    .fetch_all(pool)
    .await?;

    Ok(Json(SessionDetail {
        session: row.into(),
        events,
        meter_values,
    }))
}

pub async fn openapi_json() -> impl IntoResponse {
    Json(ApiDoc::openapi())
}
