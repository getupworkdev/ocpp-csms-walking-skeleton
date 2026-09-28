//! OCPP-J RPC framing (OCPP-J 1.6 section 4).
//!
//! ```text
//! [2, "<uniqueId>", "<Action>", {payload}]                      CALL
//! [3, "<uniqueId>", {payload}]                                  CALLRESULT
//! [4, "<uniqueId>", "<errorCode>", "<description>", {details}]  CALLERROR
//! ```

use serde_json::{Value, json};

const CALL: u64 = 2;
const CALL_RESULT: u64 = 3;
const CALL_ERROR: u64 = 4;

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Call {
        unique_id: String,
        action: String,
        payload: Value,
    },
    CallResult {
        unique_id: String,
        payload: Value,
    },
    CallError(CallError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallError {
    pub unique_id: String,
    pub code: ErrorCode,
    pub description: String,
    pub details: Value,
}

/// Error codes from OCPP-J 1.6 section 4.2.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    NotImplemented,
    NotSupported,
    InternalError,
    ProtocolError,
    SecurityError,
    FormationViolation,
    PropertyConstraintViolation,
    OccurenceConstraintViolation,
    TypeConstraintViolation,
    GenericError,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotImplemented => "NotImplemented",
            Self::NotSupported => "NotSupported",
            Self::InternalError => "InternalError",
            Self::ProtocolError => "ProtocolError",
            Self::SecurityError => "SecurityError",
            Self::FormationViolation => "FormationViolation",
            Self::PropertyConstraintViolation => "PropertyConstraintViolation",
            // Misspelt in the spec; chargers send it this way.
            Self::OccurenceConstraintViolation => "OccurenceConstraintViolation",
            Self::TypeConstraintViolation => "TypeConstraintViolation",
            Self::GenericError => "GenericError",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "NotImplemented" => Self::NotImplemented,
            "NotSupported" => Self::NotSupported,
            "InternalError" => Self::InternalError,
            "ProtocolError" => Self::ProtocolError,
            "SecurityError" => Self::SecurityError,
            "FormationViolation" => Self::FormationViolation,
            "PropertyConstraintViolation" => Self::PropertyConstraintViolation,
            "OccurenceConstraintViolation" => Self::OccurenceConstraintViolation,
            "TypeConstraintViolation" => Self::TypeConstraintViolation,
            _ => Self::GenericError,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FrameError {
    #[error("frame is not valid JSON: {0}")]
    NotJson(String),
    #[error("frame is not a JSON array with a message type id")]
    NotAnArray,
    /// The frame had a readable unique id, so the error can be sent back to the caller.
    #[error("malformed frame {unique_id}: {reason}")]
    Malformed { unique_id: String, reason: String },
    #[error("unknown message type id {0}")]
    UnknownType(u64),
}

impl Frame {
    pub fn call(unique_id: impl Into<String>, action: impl Into<String>, payload: Value) -> Self {
        Self::Call {
            unique_id: unique_id.into(),
            action: action.into(),
            payload,
        }
    }

    pub fn unique_id(&self) -> &str {
        match self {
            Self::Call { unique_id, .. } | Self::CallResult { unique_id, .. } => unique_id,
            Self::CallError(e) => &e.unique_id,
        }
    }

    pub fn parse(text: &str) -> Result<Self, FrameError> {
        let value: Value =
            serde_json::from_str(text).map_err(|e| FrameError::NotJson(e.to_string()))?;
        let items = value.as_array().ok_or(FrameError::NotAnArray)?;
        let type_id = items
            .first()
            .and_then(Value::as_u64)
            .ok_or(FrameError::NotAnArray)?;
        let unique_id = items
            .get(1)
            .and_then(Value::as_str)
            .ok_or(FrameError::NotAnArray)?
            .to_owned();
        let malformed = |reason: &str| FrameError::Malformed {
            unique_id: unique_id.clone(),
            reason: reason.to_owned(),
        };

        match type_id {
            CALL => {
                if items.len() != 4 {
                    return Err(malformed("CALL must have 4 elements"));
                }
                let action = items[2]
                    .as_str()
                    .ok_or_else(|| malformed("action must be a string"))?
                    .to_owned();
                Ok(Self::Call {
                    unique_id: unique_id.clone(),
                    action,
                    payload: items[3].clone(),
                })
            }
            CALL_RESULT => {
                if items.len() != 3 {
                    return Err(malformed("CALLRESULT must have 3 elements"));
                }
                Ok(Self::CallResult {
                    unique_id: unique_id.clone(),
                    payload: items[2].clone(),
                })
            }
            CALL_ERROR => {
                if items.len() != 5 {
                    return Err(malformed("CALLERROR must have 5 elements"));
                }
                Ok(Self::CallError(CallError {
                    unique_id: unique_id.clone(),
                    code: ErrorCode::parse(items[2].as_str().unwrap_or_default()),
                    description: items[3].as_str().unwrap_or_default().to_owned(),
                    details: items[4].clone(),
                }))
            }
            other => Err(FrameError::UnknownType(other)),
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::Call {
                unique_id,
                action,
                payload,
            } => json!([CALL, unique_id, action, payload]),
            Self::CallResult { unique_id, payload } => json!([CALL_RESULT, unique_id, payload]),
            Self::CallError(e) => json!([
                CALL_ERROR,
                e.unique_id,
                e.code.as_str(),
                e.description,
                e.details
            ]),
        }
    }
}

impl std::fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_json())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_round_trips() {
        let text = r#"[2,"abc","Heartbeat",{}]"#;
        let frame = Frame::parse(text).unwrap();
        assert_eq!(frame, Frame::call("abc", "Heartbeat", json!({})));
        assert_eq!(frame.to_string(), text);
    }

    #[test]
    fn call_error_round_trips() {
        let text = r#"[4,"x","NotImplemented","nope",{}]"#;
        let Frame::CallError(e) = Frame::parse(text).unwrap() else {
            panic!("expected CallError")
        };
        assert_eq!(e.code, ErrorCode::NotImplemented);
        assert_eq!(Frame::CallError(e).to_string(), text);
    }

    #[test]
    fn malformed_call_keeps_unique_id() {
        let err = Frame::parse(r#"[2,"id-1","Heartbeat"]"#).unwrap_err();
        assert!(matches!(err, FrameError::Malformed { unique_id, .. } if unique_id == "id-1"));
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(matches!(Frame::parse("{}"), Err(FrameError::NotAnArray)));
        assert!(matches!(Frame::parse("nope"), Err(FrameError::NotJson(_))));
        assert_eq!(
            Frame::parse(r#"[9,"a"]"#).unwrap_err(),
            FrameError::UnknownType(9)
        );
    }
}
