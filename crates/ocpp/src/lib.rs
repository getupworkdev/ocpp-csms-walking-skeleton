//! OCPP 1.6J wire types: the RPC framing and the six messages this CSMS handles.
//!
//! Only the fields the skeleton uses are modelled. Unknown fields are ignored on
//! deserialise, which is what the spec asks of receivers anyway.

pub mod frame;
pub mod messages;

pub use frame::{CallError, ErrorCode, Frame, FrameError};

/// WebSocket subprotocol negotiated for OCPP 1.6J.
pub const SUBPROTOCOL: &str = "ocpp1.6";
