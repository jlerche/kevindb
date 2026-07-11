use anyhow::anyhow;
use uuid::Uuid;

use crate::ApiError;

pub(super) fn tenant_uuid() -> Uuid {
    Uuid::from_u128(0x4b4556494e4440008000000000000001)
}

pub(super) fn canonical_uuid(value: &str, field: &str) -> Result<String, ApiError> {
    Uuid::parse_str(value)
        .map(|uuid| uuid.to_string())
        .map_err(|error| ApiError::bad_request(format!("{field} must be a UUID: {error}")))
}

pub(super) fn uuid_to_otel_trace_id(value: &str, field: &str) -> Result<String, ApiError> {
    Uuid::parse_str(value)
        .map(|uuid| uuid.simple().to_string())
        .map_err(|error| ApiError::bad_request(format!("{field} must be a UUID: {error}")))
}

pub(super) fn uuid_simple(value: &str) -> String {
    Uuid::parse_str(value)
        .expect("run IDs are canonicalized before conversion")
        .simple()
        .to_string()
}

pub(super) fn uuid_to_span_id(value: &str) -> String {
    uuid_simple(value).chars().take(16).collect()
}

pub(super) fn otel_trace_id_to_uuid(trace_id: &str) -> Result<String, ApiError> {
    Uuid::parse_str(trace_id)
        .map(|uuid| uuid.to_string())
        .map_err(|error| {
            ApiError::from(anyhow!(
                "stored trace ID is not a canonical UUID or OTLP hexadecimal ID: {error}"
            ))
        })
}

pub(super) fn normalize_trace_filter(trace_id: Option<String>) -> Result<Option<String>, ApiError> {
    trace_id
        .map(|trace_id| {
            if trace_id.len() == 32 && trace_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                if trace_id.bytes().all(|byte| byte == b'0') {
                    return Err(ApiError::bad_request(
                        "trace_id must not be all zeroes".to_owned(),
                    ));
                }
                return Ok(trace_id.to_ascii_lowercase());
            }
            uuid_to_otel_trace_id(&trace_id, "trace_id")
        })
        .transpose()
}
