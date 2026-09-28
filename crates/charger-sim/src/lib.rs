//! A scriptable OCPP 1.6J charge point.
//!
//! It behaves like a charger with an offline transaction queue: while the
//! socket is down, transaction messages (MeterValues, StopTransaction) are
//! kept with their original uniqueId and timestamp, and sent in order when it
//! reconnects. It can also drop the connection straight after sending a
//! message, before the answer arrives, which is how a real charger ends up
//! resending something the CSMS already processed.

use std::collections::VecDeque;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use ocpp::messages::{
    Action, AuthorizeRequest, AuthorizeResponse, BootNotificationRequest, BootNotificationResponse,
    IdTagInfo, MeterValue, MeterValuesRequest, SampledValue, StartTransactionRequest,
    StopTransactionRequest,
};
use ocpp::{CallError, Frame};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum SimError {
    #[error("websocket: {0}")]
    Ws(#[from] tungstenite::Error),
    #[error("not connected")]
    Offline,
    #[error("connection closed while waiting for a response")]
    Closed,
    #[error("no response within {RESPONSE_TIMEOUT:?}")]
    Timeout,
    #[error("CSMS returned {}: {}", .0.code.as_str(), .0.description)]
    CallError(CallError),
    #[error("bad frame from CSMS: {0}")]
    Frame(#[from] ocpp::FrameError),
    #[error("unexpected payload: {0}")]
    Payload(#[from] serde_json::Error),
}

/// Whether a transaction message went out now or is waiting in the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    Queued,
}

#[derive(Debug, Clone)]
struct QueuedCall {
    unique_id: String,
    action: &'static str,
    payload: Value,
}

pub struct ChargePoint {
    url: String,
    ws: Option<Ws>,
    queue: VecDeque<QueuedCall>,
    drop_after_next_send: bool,
}

impl ChargePoint {
    /// `base_url` is the CSMS OCPP endpoint without the id, e.g.
    /// `ws://localhost:8180/ocpp`.
    pub async fn connect(base_url: &str, charger_id: &str) -> Result<Self, SimError> {
        let mut cp = Self {
            url: format!("{}/{}", base_url.trim_end_matches('/'), charger_id),
            ws: None,
            queue: VecDeque::new(),
            drop_after_next_send: false,
        };
        cp.open().await?;
        Ok(cp)
    }

    async fn open(&mut self) -> Result<(), SimError> {
        let mut request = self.url.as_str().into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(ocpp::SUBPROTOCOL),
        );
        let (ws, _) = tokio_tungstenite::connect_async(request).await?;
        self.ws = Some(ws);
        Ok(())
    }

    pub fn is_online(&self) -> bool {
        self.ws.is_some()
    }

    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    /// Lose the connection without a close handshake, like a cellular drop.
    pub fn go_offline(&mut self) {
        self.ws = None;
    }

    /// Send the next message, then lose the connection before reading the
    /// answer. The message stays queued and is resent, with the same
    /// uniqueId, on reconnect.
    pub fn drop_connection_after_next_send(&mut self) {
        self.drop_after_next_send = true;
    }

    /// Reconnect and send everything that queued up while offline, oldest
    /// first. Returns how many queued messages were delivered.
    pub async fn reconnect(&mut self) -> Result<usize, SimError> {
        self.open().await?;
        self.flush().await
    }

    async fn flush(&mut self) -> Result<usize, SimError> {
        let mut sent = 0;
        while let Some(call) = self.queue.front().cloned() {
            // Only dequeue once the CSMS has answered, so a drop mid-flush
            // leaves the message for next time.
            self.call_raw(&call.unique_id, call.action, call.payload)
                .await?;
            self.queue.pop_front();
            sent += 1;
        }
        Ok(sent)
    }

    /// Send one CALL and wait for its answer. CallError comes back as a
    /// frame, not an Err, so tests can inspect it.
    pub async fn call_raw(
        &mut self,
        unique_id: &str,
        action: &str,
        payload: Value,
    ) -> Result<Frame, SimError> {
        let ws = self.ws.as_mut().ok_or(SimError::Offline)?;
        let frame = Frame::call(unique_id, action, payload);
        ws.send(Message::text(frame.to_string())).await?;

        let wait = async {
            while let Some(msg) = ws.next().await {
                let Message::Text(text) = msg? else { continue };
                let frame = Frame::parse(text.as_str())?;
                if frame.unique_id() == unique_id {
                    return Ok(frame);
                }
            }
            Err(SimError::Closed)
        };
        match tokio::time::timeout(RESPONSE_TIMEOUT, wait).await {
            Ok(result) => result,
            Err(_) => Err(SimError::Timeout),
        }
    }

    /// A typed call that must be answered now (not queued).
    pub async fn call<A: Action>(&mut self, req: &A) -> Result<A::Response, SimError> {
        let unique_id = new_unique_id();
        match self
            .call_raw(&unique_id, A::NAME, serde_json::to_value(req)?)
            .await?
        {
            Frame::CallResult { payload, .. } => Ok(serde_json::from_value(payload)?),
            Frame::CallError(e) => Err(SimError::CallError(e)),
            Frame::Call { .. } => Err(SimError::Closed),
        }
    }

    /// Transaction messages go through here: sent now if online and nothing is
    /// queued ahead of them, otherwise queued so ordering is preserved.
    async fn send_transaction_message<A: Action>(&mut self, req: &A) -> Result<Delivery, SimError> {
        let call = QueuedCall {
            unique_id: new_unique_id(),
            action: A::NAME,
            payload: serde_json::to_value(req)?,
        };
        if !self.is_online() || !self.queue.is_empty() {
            self.queue.push_back(call);
            return Ok(Delivery::Queued);
        }

        if self.drop_after_next_send {
            self.drop_after_next_send = false;
            if let Some(ws) = self.ws.as_mut() {
                ws.send(Message::text(
                    Frame::call(&call.unique_id, call.action, call.payload.clone()).to_string(),
                ))
                .await?;
                ws.flush().await?;
            }
            self.queue.push_back(call);
            self.go_offline();
            return Ok(Delivery::Queued);
        }

        match self
            .call_raw(&call.unique_id, call.action, call.payload.clone())
            .await
        {
            Ok(Frame::CallError(e)) => Err(SimError::CallError(e)),
            Ok(_) => Ok(Delivery::Sent),
            // Lost the link mid-call: keep it for the reconnect.
            Err(SimError::Ws(_) | SimError::Closed | SimError::Timeout) => {
                self.queue.push_back(call);
                self.go_offline();
                Ok(Delivery::Queued)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn boot(&mut self) -> Result<BootNotificationResponse, SimError> {
        self.call(&BootNotificationRequest {
            charge_point_vendor: "Skeleton".into(),
            charge_point_model: "SimCharger 1".into(),
            charge_point_serial_number: Some("SIM-0001".into()),
            firmware_version: Some(env!("CARGO_PKG_VERSION").into()),
        })
        .await
    }

    pub async fn authorize(&mut self, id_tag: &str) -> Result<IdTagInfo, SimError> {
        let conf: AuthorizeResponse = self
            .call(&AuthorizeRequest {
                id_tag: id_tag.into(),
            })
            .await?;
        Ok(conf.id_tag_info)
    }

    /// Start a transaction and return the id the CSMS assigned.
    pub async fn start_transaction(
        &mut self,
        connector_id: i32,
        id_tag: &str,
        meter_start_wh: i64,
        timestamp: DateTime<Utc>,
    ) -> Result<(i32, IdTagInfo), SimError> {
        let conf = self
            .call(&StartTransactionRequest {
                connector_id,
                id_tag: id_tag.into(),
                meter_start: meter_start_wh,
                reservation_id: None,
                timestamp,
            })
            .await?;
        Ok((conf.transaction_id, conf.id_tag_info))
    }

    pub async fn meter_values(
        &mut self,
        connector_id: i32,
        transaction_id: i32,
        register_wh: i64,
        timestamp: DateTime<Utc>,
    ) -> Result<Delivery, SimError> {
        self.send_transaction_message(&MeterValuesRequest {
            connector_id,
            transaction_id: Some(transaction_id),
            meter_value: vec![MeterValue {
                timestamp,
                sampled_value: vec![SampledValue::energy_wh(register_wh)],
            }],
        })
        .await
    }

    pub async fn stop_transaction(
        &mut self,
        transaction_id: i32,
        meter_stop_wh: i64,
        timestamp: DateTime<Utc>,
        reason: &str,
    ) -> Result<Delivery, SimError> {
        self.send_transaction_message(&StopTransactionRequest {
            id_tag: None,
            meter_stop: meter_stop_wh,
            timestamp,
            transaction_id,
            reason: Some(reason.into()),
            transaction_data: None,
        })
        .await
    }
}

pub fn new_unique_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A scripted charging session.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub id_tag: String,
    pub connector_id: i32,
    pub meter_start_wh: i64,
    pub samples: u32,
    pub wh_per_sample: i64,
    /// Real time between samples.
    pub interval: Duration,
    pub offline: Option<Offline>,
}

/// Go offline part-way through the session and come back after it ended.
#[derive(Debug, Clone)]
pub struct Offline {
    /// Samples sent normally before the link drops.
    pub after_samples: u32,
    /// How long to stay offline after the session ends before reconnecting.
    pub for_duration: Duration,
    /// Drop the link straight after sending the next sample, so the CSMS
    /// processes it but the charger never sees the answer and resends it.
    pub lose_ack: bool,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            id_tag: "DEMO-TAG-1".into(),
            connector_id: 1,
            meter_start_wh: 10_000,
            samples: 5,
            wh_per_sample: 1_500,
            interval: Duration::from_millis(200),
            offline: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub transaction_id: i32,
    pub meter_start_wh: i64,
    pub meter_stop_wh: i64,
    pub stopped_at: DateTime<Utc>,
    /// Messages that went out from the offline queue after reconnecting.
    pub replayed: usize,
}

/// Boot, authorise, start, sample, stop; optionally losing the connection in
/// the middle and replaying the backlog at the end.
pub async fn run(cp: &mut ChargePoint, scenario: &Scenario) -> Result<Report, SimError> {
    cp.boot().await?;
    let tag = cp.authorize(&scenario.id_tag).await?;
    tracing::info!(status = ?tag.status, "authorised");

    let (transaction_id, _) = cp
        .start_transaction(
            scenario.connector_id,
            &scenario.id_tag,
            scenario.meter_start_wh,
            Utc::now(),
        )
        .await?;
    tracing::info!(transaction_id, "transaction started");

    let mut register = scenario.meter_start_wh;
    for n in 1..=scenario.samples {
        tokio::time::sleep(scenario.interval).await;
        if let Some(off) = &scenario.offline
            && n == off.after_samples + 1
        {
            if off.lose_ack {
                cp.drop_connection_after_next_send();
            } else {
                cp.go_offline();
            }
            tracing::info!("connection lost");
        }
        register += scenario.wh_per_sample;
        let delivery = cp
            .meter_values(scenario.connector_id, transaction_id, register, Utc::now())
            .await?;
        tracing::info!(register, ?delivery, "meter values");
    }

    let stopped_at = Utc::now();
    let delivery = cp
        .stop_transaction(transaction_id, register, stopped_at, "Local")
        .await?;
    tracing::info!(meter_stop = register, ?delivery, "transaction stopped");

    let mut replayed = 0;
    if let Some(off) = &scenario.offline {
        tracing::info!(
            queued = cp.queued(),
            "offline, waiting {:?}",
            off.for_duration
        );
        tokio::time::sleep(off.for_duration).await;
        replayed = cp.reconnect().await?;
        tracing::info!(replayed, "reconnected and replayed backlog");
    }

    Ok(Report {
        transaction_id,
        meter_start_wh: scenario.meter_start_wh,
        meter_stop_wh: register,
        stopped_at,
        replayed,
    })
}
