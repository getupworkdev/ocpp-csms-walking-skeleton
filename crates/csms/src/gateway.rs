//! OCPP-J WebSocket endpoint: `GET /ocpp/{charge_point_id}` with the
//! `ocpp1.6` subprotocol.
//!
//! Calls on one connection are handled one at a time, in order. OCPP only
//! allows one outstanding CALL per direction, so a well-behaved charger never
//! has more than one in flight anyway.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ocpp::{CallError, ErrorCode, Frame, FrameError};
use serde_json::json;

use crate::AppState;

const MAX_ID_LEN: usize = 64;

pub async fn upgrade(
    Path(charger_id): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    if charger_id.is_empty() || charger_id.len() > MAX_ID_LEN {
        return (StatusCode::BAD_REQUEST, "invalid charge point id").into_response();
    }
    let ws = ws.protocols([ocpp::SUBPROTOCOL]);
    if ws.selected_protocol().is_none() {
        return (
            StatusCode::BAD_REQUEST,
            "expected Sec-WebSocket-Protocol: ocpp1.6",
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| connection(state, charger_id, socket))
}

async fn connection(state: AppState, charger_id: String, mut socket: WebSocket) {
    if let Err(e) = state.processor.touch_charger(&charger_id).await {
        tracing::error!(charger_id, error = %e, "could not register charger, closing");
        return;
    }
    let generation = state.connections.connect(&charger_id);
    tracing::info!(charger_id, "connected");

    while let Some(msg) = socket.recv().await {
        let text = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => break,
            // Pings are answered by the WebSocket layer; OCPP-J is text only.
            Ok(_) => continue,
            Err(e) => {
                tracing::info!(charger_id, error = %e, "socket error");
                break;
            }
        };

        let reply = match Frame::parse(text.as_str()) {
            Ok(Frame::Call {
                unique_id,
                action,
                payload,
            }) => Some(
                state
                    .processor
                    .handle_call(&charger_id, &unique_id, &action, payload)
                    .await,
            ),
            // The CSMS never sends calls in this skeleton, so there is nothing
            // waiting on a result.
            Ok(frame @ (Frame::CallResult { .. } | Frame::CallError(_))) => {
                tracing::debug!(charger_id, %frame, "unsolicited response ignored");
                None
            }
            Err(FrameError::Malformed { unique_id, reason }) => Some(Frame::CallError(CallError {
                unique_id,
                code: ErrorCode::ProtocolError,
                description: reason,
                details: json!({}),
            })),
            // Without a unique id there is no way to address a reply.
            Err(e) => {
                tracing::warn!(charger_id, error = %e, "unreadable frame dropped");
                None
            }
        };

        if let Some(reply) = reply
            && socket.send(Message::text(reply.to_string())).await.is_err()
        {
            break;
        }
    }

    state.connections.disconnect(&charger_id, generation);
    tracing::info!(charger_id, "disconnected");
}

/// Which chargers currently have a socket open. A charger that reconnects
/// before its old socket is noticed as dead gets a new generation, so the old
/// one closing does not mark it offline.
#[derive(Default)]
pub struct Connections {
    next: AtomicU64,
    open: Mutex<HashMap<String, u64>>,
}

impl Connections {
    fn connect(&self, charger_id: &str) -> u64 {
        let generation = self.next.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut open) = self.open.lock() {
            open.insert(charger_id.to_owned(), generation);
        }
        generation
    }

    fn disconnect(&self, charger_id: &str, generation: u64) {
        if let Ok(mut open) = self.open.lock()
            && open.get(charger_id) == Some(&generation)
        {
            open.remove(charger_id);
        }
    }

    pub fn is_connected(&self, charger_id: &str) -> bool {
        self.open
            .lock()
            .map(|open| open.contains_key(charger_id))
            .unwrap_or(false)
    }
}
