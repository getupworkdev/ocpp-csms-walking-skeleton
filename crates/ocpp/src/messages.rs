//! Payloads for the charge-point-initiated messages this CSMS handles.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Ties a request payload to its action name and response payload.
pub trait Action: Serialize + DeserializeOwned {
    const NAME: &'static str;
    type Response: Serialize + DeserializeOwned;
}

macro_rules! action {
    ($req:ty => $conf:ty, $name:literal) => {
        impl Action for $req {
            const NAME: &'static str = $name;
            type Response = $conf;
        }
    };
}

action!(BootNotificationRequest => BootNotificationResponse, "BootNotification");
action!(HeartbeatRequest => HeartbeatResponse, "Heartbeat");
action!(AuthorizeRequest => AuthorizeResponse, "Authorize");
action!(StartTransactionRequest => StartTransactionResponse, "StartTransaction");
action!(MeterValuesRequest => MeterValuesResponse, "MeterValues");
action!(StopTransactionRequest => StopTransactionResponse, "StopTransaction");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootNotificationRequest {
    pub charge_point_vendor: String,
    pub charge_point_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charge_point_serial_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firmware_version: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RegistrationStatus {
    Accepted,
    Pending,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootNotificationResponse {
    pub status: RegistrationStatus,
    pub current_time: DateTime<Utc>,
    /// Heartbeat interval in seconds.
    pub interval: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct HeartbeatRequest {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    pub current_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizeRequest {
    pub id_tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizeResponse {
    pub id_tag_info: IdTagInfo,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthorizationStatus {
    Accepted,
    Blocked,
    Expired,
    Invalid,
    ConcurrentTx,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IdTagInfo {
    pub status: AuthorizationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_date: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id_tag: Option<String>,
}

impl IdTagInfo {
    pub fn status(status: AuthorizationStatus) -> Self {
        Self {
            status,
            expiry_date: None,
            parent_id_tag: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StartTransactionRequest {
    pub connector_id: i32,
    pub id_tag: String,
    /// Energy register at start, in Wh.
    pub meter_start: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_id: Option<i32>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StartTransactionResponse {
    pub id_tag_info: IdTagInfo,
    pub transaction_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MeterValuesRequest {
    pub connector_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<i32>,
    pub meter_value: Vec<MeterValue>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MeterValuesResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MeterValue {
    pub timestamp: DateTime<Utc>,
    pub sampled_value: Vec<SampledValue>,
}

/// One sample. Everything but `value` is optional on the wire; the spec
/// defaults are applied by the accessor methods, not on deserialise, so what
/// the charger actually sent can be stored as-is.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SampledValue {
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurand: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

pub const ENERGY_IMPORT_REGISTER: &str = "Energy.Active.Import.Register";

impl SampledValue {
    pub fn energy_wh(wh: i64) -> Self {
        Self {
            value: wh.to_string(),
            context: Some("Sample.Periodic".into()),
            measurand: Some(ENERGY_IMPORT_REGISTER.into()),
            phase: None,
            location: None,
            unit: Some("Wh".into()),
        }
    }

    pub fn measurand(&self) -> &str {
        self.measurand.as_deref().unwrap_or(ENERGY_IMPORT_REGISTER)
    }

    /// The import register reading in Wh, if this sample is one.
    pub fn import_register_wh(&self) -> Option<i64> {
        if self.measurand() != ENERGY_IMPORT_REGISTER {
            return None;
        }
        let v: f64 = self.value.trim().parse().ok()?;
        let wh = match self.unit.as_deref().unwrap_or("Wh") {
            "Wh" => v,
            "kWh" => v * 1000.0,
            _ => return None,
        };
        wh.is_finite().then(|| wh.round() as i64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StopTransactionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_tag: Option<String>,
    /// Energy register at stop, in Wh.
    pub meter_stop: i64,
    pub timestamp: DateTime<Utc>,
    pub transaction_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_data: Option<Vec<MeterValue>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StopTransactionResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_tag_info: Option<IdTagInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn start_transaction_uses_spec_field_names() {
        let req: StartTransactionRequest = serde_json::from_value(json!({
            "connectorId": 1,
            "idTag": "TAG",
            "meterStart": 1200,
            "timestamp": "2026-01-01T10:00:00Z"
        }))
        .unwrap();
        assert_eq!(req.meter_start, 1200);
        assert_eq!(req.reservation_id, None);
    }

    #[test]
    fn energy_register_handles_units_and_defaults() {
        let mut sv = SampledValue {
            value: "12.5".into(),
            context: None,
            measurand: None,
            phase: None,
            location: None,
            unit: Some("kWh".into()),
        };
        assert_eq!(sv.import_register_wh(), Some(12_500));
        sv.unit = None;
        assert_eq!(sv.import_register_wh(), Some(13));
        sv.measurand = Some("Power.Active.Import".into());
        assert_eq!(sv.import_register_wh(), None);
    }
}
