//! Handles charger-initiated OCPP calls against Postgres.
//!
//! Each call runs in one database transaction:
//!
//! 1. insert `(charger_id, unique_id)` into `ocpp_messages`
//! 2. if that conflicts, this is a replay: return the stored response and stop
//! 3. otherwise handle the message and store the response next to it
//!
//! Two copies of the same message racing each other serialise on the primary
//! key: the second insert waits for the first transaction, then conflicts and
//! reads the committed response. If the first rolls back (database error), the
//! second goes ahead and handles it, so an InternalError is always retryable.

use chrono::{DateTime, Utc};
use ocpp::messages::{
    Action, AuthorizationStatus, AuthorizeRequest, AuthorizeResponse, BootNotificationRequest,
    BootNotificationResponse, HeartbeatResponse, IdTagInfo, MeterValue, MeterValuesRequest,
    MeterValuesResponse, RegistrationStatus, StartTransactionRequest, StartTransactionResponse,
    StopTransactionRequest, StopTransactionResponse,
};
use ocpp::{CallError, ErrorCode, Frame};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::types::Json;
use sqlx::{PgConnection, PgPool};

use crate::tariff::Tariff;

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error("{0}")]
    Config(&'static str),
    #[error("stored response is not a valid frame: {0}")]
    CorruptResponse(#[from] ocpp::FrameError),
}

#[derive(Clone)]
pub struct Processor {
    pool: PgPool,
    heartbeat_interval_secs: i32,
}

/// What a handler produced: the frame to send back, and the transaction it
/// concerned (recorded on the message log for tracing a session's history).
struct Outcome {
    frame: Frame,
    transaction_id: Option<i32>,
}

impl Processor {
    pub fn new(pool: PgPool, heartbeat_interval_secs: i32) -> Self {
        Self {
            pool,
            heartbeat_interval_secs,
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Record that a charger is connected, creating it on first contact.
    pub async fn touch_charger(&self, charger_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "insert into chargers (id) values ($1)
             on conflict (id) do update set last_seen_at = now()",
        )
        .bind(charger_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Handle one CALL and return the frame to answer it with. Never fails:
    /// anything unexpected becomes an InternalError that is not stored, so the
    /// charger's retry gets a fresh attempt.
    pub async fn handle_call(
        &self,
        charger_id: &str,
        unique_id: &str,
        action: &str,
        payload: Value,
    ) -> Frame {
        match self
            .try_handle(charger_id, unique_id, action, payload)
            .await
        {
            Ok(frame) => frame,
            Err(e) => {
                tracing::error!(charger_id, unique_id, action, error = %e, "call failed");
                call_error(unique_id, ErrorCode::InternalError, "internal error")
            }
        }
    }

    async fn try_handle(
        &self,
        charger_id: &str,
        unique_id: &str,
        action: &str,
        payload: Value,
    ) -> Result<Frame, ProcessError> {
        // Heartbeats have no effect beyond a timestamp and arrive forever, so
        // they are answered directly rather than logged.
        if action == "Heartbeat" {
            sqlx::query(
                "update chargers set last_heartbeat_at = now(), last_seen_at = now() where id = $1",
            )
            .bind(charger_id)
            .execute(&self.pool)
            .await?;
            return Ok(result(
                unique_id,
                HeartbeatResponse {
                    current_time: Utc::now(),
                },
            ));
        }

        let mut tx = self.pool.begin().await?;

        let inserted = sqlx::query(
            "insert into ocpp_messages (charger_id, unique_id, action, payload)
             values ($1, $2, $3, $4)
             on conflict (charger_id, unique_id) do nothing",
        )
        .bind(charger_id)
        .bind(unique_id)
        .bind(action)
        .bind(Json(&payload))
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;

        if !inserted {
            let frame = replay(&mut tx, charger_id, unique_id, action, &payload).await?;
            tx.commit().await?;
            return Ok(frame);
        }

        let outcome = self
            .dispatch(&mut tx, charger_id, unique_id, action, payload)
            .await?;

        sqlx::query(
            "update ocpp_messages set response = $3, transaction_id = $4
             where charger_id = $1 and unique_id = $2",
        )
        .bind(charger_id)
        .bind(unique_id)
        .bind(Json(outcome.frame.to_json()))
        .bind(outcome.transaction_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(outcome.frame)
    }

    async fn dispatch(
        &self,
        conn: &mut PgConnection,
        charger_id: &str,
        unique_id: &str,
        action: &str,
        payload: Value,
    ) -> Result<Outcome, ProcessError> {
        macro_rules! parse {
            ($t:ty) => {
                match serde_json::from_value::<$t>(payload) {
                    Ok(req) => req,
                    Err(e) => {
                        return Ok(Outcome {
                            frame: call_error(
                                unique_id,
                                ErrorCode::FormationViolation,
                                &format!("invalid {} payload: {e}", <$t as Action>::NAME),
                            ),
                            transaction_id: None,
                        });
                    }
                }
            };
        }

        match action {
            BootNotificationRequest::NAME => {
                let req = parse!(BootNotificationRequest);
                let conf = self.boot(conn, charger_id, req).await?;
                Ok(Outcome {
                    frame: result(unique_id, conf),
                    transaction_id: None,
                })
            }
            AuthorizeRequest::NAME => {
                let req = parse!(AuthorizeRequest);
                let id_tag_info = id_tag_info(conn, &req.id_tag).await?;
                Ok(Outcome {
                    frame: result(unique_id, AuthorizeResponse { id_tag_info }),
                    transaction_id: None,
                })
            }
            StartTransactionRequest::NAME => {
                let req = parse!(StartTransactionRequest);
                let conf = start_transaction(conn, charger_id, req).await?;
                Ok(Outcome {
                    transaction_id: Some(conf.transaction_id),
                    frame: result(unique_id, conf),
                })
            }
            MeterValuesRequest::NAME => {
                let req = parse!(MeterValuesRequest);
                insert_meter_values(
                    conn,
                    charger_id,
                    unique_id,
                    req.connector_id,
                    req.transaction_id,
                    &req.meter_value,
                )
                .await?;
                Ok(Outcome {
                    frame: result(unique_id, MeterValuesResponse {}),
                    transaction_id: req.transaction_id,
                })
            }
            StopTransactionRequest::NAME => {
                let req = parse!(StopTransactionRequest);
                let transaction_id = req.transaction_id;
                let conf = stop_transaction(conn, charger_id, unique_id, req).await?;
                Ok(Outcome {
                    frame: result(unique_id, conf),
                    transaction_id: Some(transaction_id),
                })
            }
            other => Ok(Outcome {
                frame: call_error(
                    unique_id,
                    ErrorCode::NotImplemented,
                    &format!("{other} is not implemented"),
                ),
                transaction_id: None,
            }),
        }
    }

    async fn boot(
        &self,
        conn: &mut PgConnection,
        charger_id: &str,
        req: BootNotificationRequest,
    ) -> Result<BootNotificationResponse, ProcessError> {
        sqlx::query(
            "update chargers set vendor = $2, model = $3, serial_number = $4,
                 firmware_version = $5, last_boot_at = now(), last_seen_at = now()
             where id = $1",
        )
        .bind(charger_id)
        .bind(&req.charge_point_vendor)
        .bind(&req.charge_point_model)
        .bind(&req.charge_point_serial_number)
        .bind(&req.firmware_version)
        .execute(conn)
        .await?;

        Ok(BootNotificationResponse {
            status: RegistrationStatus::Accepted,
            current_time: Utc::now(),
            interval: self.heartbeat_interval_secs,
        })
    }
}

/// A uniqueId we have seen before. Same message: answer exactly as last time.
/// Different message under a reused id: refuse rather than silently drop it,
/// so a charger with a broken id generator shows up in the logs.
async fn replay(
    conn: &mut PgConnection,
    charger_id: &str,
    unique_id: &str,
    action: &str,
    payload: &Value,
) -> Result<Frame, ProcessError> {
    let (stored_action, Json(stored_payload), response): (
        String,
        Json<Value>,
        Option<Json<Value>>,
    ) = sqlx::query_as(
        "update ocpp_messages set duplicates = duplicates + 1
         where charger_id = $1 and unique_id = $2
         returning action, payload, response",
    )
    .bind(charger_id)
    .bind(unique_id)
    .fetch_one(conn)
    .await?;

    if stored_action != action || &stored_payload != payload {
        tracing::warn!(
            charger_id,
            unique_id,
            action,
            stored_action,
            "uniqueId reused for a different message"
        );
        return Ok(call_error(
            unique_id,
            ErrorCode::ProtocolError,
            "uniqueId was already used for a different message",
        ));
    }

    tracing::info!(
        charger_id,
        unique_id,
        action,
        "duplicate message, replaying stored response"
    );
    match response {
        Some(Json(frame)) => Ok(Frame::from_value(&frame)?),
        // The insert and the response are written in one transaction, so a
        // committed row always has a response.
        None => Err(ProcessError::Config("message logged without a response")),
    }
}

async fn id_tag_info(conn: &mut PgConnection, id_tag: &str) -> Result<IdTagInfo, sqlx::Error> {
    let row: Option<(String, Option<DateTime<Utc>>)> =
        sqlx::query_as("select status, expires_at from id_tags where id_tag = $1")
            .bind(id_tag)
            .fetch_optional(conn)
            .await?;

    let Some((status, expires_at)) = row else {
        return Ok(IdTagInfo::status(AuthorizationStatus::Invalid));
    };
    let status = match status.as_str() {
        _ if expires_at.is_some_and(|at| at <= Utc::now()) => AuthorizationStatus::Expired,
        "Accepted" => AuthorizationStatus::Accepted,
        "Blocked" => AuthorizationStatus::Blocked,
        "Expired" => AuthorizationStatus::Expired,
        _ => AuthorizationStatus::Invalid,
    };
    Ok(IdTagInfo {
        status,
        expiry_date: expires_at,
        parent_id_tag: None,
    })
}

/// The CSMS allocates the transaction id. OCPP 1.6 expects one even when the
/// tag is refused (the charger then ends the session itself), so the row is
/// always created and the tag status recorded on it.
async fn start_transaction(
    conn: &mut PgConnection,
    charger_id: &str,
    req: StartTransactionRequest,
) -> Result<StartTransactionResponse, ProcessError> {
    let id_tag_info = id_tag_info(conn, &req.id_tag).await?;

    let tariff: Option<(i32, String, i64, i64)> = sqlx::query_as(
        "select id, currency::text, price_per_kwh_minor, session_fee_minor from tariffs where active",
    )
    .fetch_optional(&mut *conn)
    .await?;
    let (tariff_id, currency, price_per_kwh_minor, session_fee_minor) =
        tariff.ok_or(ProcessError::Config("no active tariff"))?;

    let (transaction_id,): (i32,) = sqlx::query_as(
        "insert into transactions (charger_id, connector_id, id_tag, id_tag_status,
             meter_start_wh, started_at, tariff_id, currency, price_per_kwh_minor, session_fee_minor)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         returning id",
    )
    .bind(charger_id)
    .bind(req.connector_id)
    .bind(&req.id_tag)
    .bind(status_str(id_tag_info.status))
    .bind(req.meter_start)
    .bind(req.timestamp)
    .bind(tariff_id)
    .bind(currency)
    .bind(price_per_kwh_minor)
    .bind(session_fee_minor)
    .fetch_one(conn)
    .await?;

    tracing::info!(
        charger_id,
        transaction_id,
        id_tag = req.id_tag,
        "transaction started"
    );
    Ok(StartTransactionResponse {
        id_tag_info,
        transaction_id,
    })
}

#[derive(sqlx::FromRow)]
struct OpenTransaction {
    connector_id: i32,
    meter_start_wh: i64,
    stopped_at: Option<DateTime<Utc>>,
    price_per_kwh_minor: i64,
    session_fee_minor: i64,
}

async fn stop_transaction(
    conn: &mut PgConnection,
    charger_id: &str,
    unique_id: &str,
    req: StopTransactionRequest,
) -> Result<StopTransactionResponse, ProcessError> {
    let id_tag_info = match &req.id_tag {
        Some(tag) => Some(id_tag_info(conn, tag).await?),
        None => None,
    };

    let row: Option<OpenTransaction> = sqlx::query_as(
        "select connector_id, meter_start_wh, stopped_at, price_per_kwh_minor, session_fee_minor
         from transactions where id = $1 and charger_id = $2
         for update",
    )
    .bind(req.transaction_id)
    .bind(charger_id)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(data) = &req.transaction_data {
        // transactionData carries no connector id; take it from the session.
        let connector_id = row.as_ref().map_or(0, |r| r.connector_id);
        insert_meter_values(
            conn,
            charger_id,
            unique_id,
            connector_id,
            Some(req.transaction_id),
            data,
        )
        .await?;
    }

    // StopTransaction.conf has no way to say "unknown transaction", and a
    // charger that gets an error will keep retrying, so both of these are
    // acknowledged and logged.
    let Some(tx) = row else {
        tracing::warn!(
            charger_id,
            transaction_id = req.transaction_id,
            "stop for unknown transaction"
        );
        return Ok(StopTransactionResponse { id_tag_info });
    };
    if tx.stopped_at.is_some() {
        tracing::warn!(
            charger_id,
            transaction_id = req.transaction_id,
            "second stop for a finished transaction, keeping the first"
        );
        return Ok(StopTransactionResponse { id_tag_info });
    }

    let priced = Tariff {
        price_per_kwh_minor: tx.price_per_kwh_minor,
        session_fee_minor: tx.session_fee_minor,
    }
    .price(tx.meter_start_wh, req.meter_stop);
    if req.meter_stop < tx.meter_start_wh {
        tracing::warn!(
            charger_id,
            transaction_id = req.transaction_id,
            meter_start_wh = tx.meter_start_wh,
            meter_stop_wh = req.meter_stop,
            "meter went backwards, charging zero energy"
        );
    }

    sqlx::query(
        "update transactions
         set meter_stop_wh = $2, stopped_at = $3, stop_reason = $4,
             energy_wh = $5, cost_minor = $6, stop_received_at = now()
         where id = $1",
    )
    .bind(req.transaction_id)
    .bind(req.meter_stop)
    .bind(req.timestamp)
    .bind(&req.reason)
    .bind(priced.energy_wh)
    .bind(priced.cost_minor)
    .execute(conn)
    .await?;

    tracing::info!(
        charger_id,
        transaction_id = req.transaction_id,
        energy_wh = priced.energy_wh,
        cost_minor = priced.cost_minor,
        "transaction stopped"
    );
    Ok(StopTransactionResponse { id_tag_info })
}

async fn insert_meter_values(
    conn: &mut PgConnection,
    charger_id: &str,
    unique_id: &str,
    connector_id: i32,
    transaction_id: Option<i32>,
    values: &[MeterValue],
) -> Result<(), sqlx::Error> {
    for mv in values {
        for sv in &mv.sampled_value {
            sqlx::query(
                "insert into meter_values (charger_id, source_unique_id, connector_id,
                     transaction_id, sampled_at, measurand, value, unit, context, phase,
                     location, energy_wh)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            )
            .bind(charger_id)
            .bind(unique_id)
            .bind(connector_id)
            .bind(transaction_id)
            .bind(mv.timestamp)
            .bind(sv.measurand())
            .bind(&sv.value)
            .bind(&sv.unit)
            .bind(&sv.context)
            .bind(&sv.phase)
            .bind(&sv.location)
            .bind(sv.import_register_wh())
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(())
}

fn status_str(status: AuthorizationStatus) -> &'static str {
    match status {
        AuthorizationStatus::Accepted => "Accepted",
        AuthorizationStatus::Blocked => "Blocked",
        AuthorizationStatus::Expired => "Expired",
        AuthorizationStatus::Invalid => "Invalid",
        AuthorizationStatus::ConcurrentTx => "ConcurrentTx",
    }
}

fn result(unique_id: &str, payload: impl Serialize) -> Frame {
    Frame::CallResult {
        unique_id: unique_id.to_owned(),
        // Serialising our own response structs cannot fail.
        payload: serde_json::to_value(payload).unwrap_or_else(|_| json!({})),
    }
}

fn call_error(unique_id: &str, code: ErrorCode, description: &str) -> Frame {
    Frame::CallError(CallError {
        unique_id: unique_id.to_owned(),
        code,
        description: description.to_owned(),
        details: json!({}),
    })
}
