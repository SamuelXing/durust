//! Workflow-data serialization formats.
//!
//! Every serialized value (workflow inputs/outputs, step outputs, messages,
//! event values) is stored as TEXT alongside a **format name** in the row's
//! `serialization` column. Recording the format lets a reader decode a value
//! regardless of who wrote it — the basis for cross-language interop on a shared
//! database via [`Serializer::Portable`].
//!
//! | Variant                  | Format name     | Wire form              | nil          |
//! |--------------------------|-----------------|------------------------|--------------|
//! | [`Serializer::Json`]     | `DBOS_JSON`     | base64(JSON) — default | `__DBOS_NIL` |
//! | [`Serializer::Portable`] | `portable_json` | plain JSON             | `null`       |
//!
//! Encoding uses the provider's configured serializer; **decoding always
//! dispatches on the stored format name**, so a value written under any known
//! format is read correctly. An unrecognized format yields a clear error rather
//! than silent corruption.

use crate::error::{Error, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;

/// Cross-language wire format: plain JSON, readable by any DBOS SDK.
pub const PORTABLE: &str = "portable_json";
/// Default wire format: base64-encoded JSON.
pub const DBOS_JSON: &str = "DBOS_JSON";
/// Sentinel the default format writes for a nil value.
const NIL_MARKER: &str = "__DBOS_NIL";

/// A serialization format for workflow data. Cheap to copy; held by each
/// provider as the format it *encodes* with (decoding is format-directed).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Serializer {
    /// `DBOS_JSON`: base64-encoded JSON. The default.
    #[default]
    Json,
    /// `portable_json`: plain JSON, readable across DBOS languages.
    Portable,
}

impl Serializer {
    /// The format name stored in the `serialization` column.
    pub fn name(self) -> &'static str {
        match self {
            Serializer::Json => DBOS_JSON,
            Serializer::Portable => PORTABLE,
        }
    }

    /// Encode a JSON value to its stored TEXT form.
    pub fn encode(self, value: &Value) -> Result<String> {
        match self {
            Serializer::Portable => {
                if value.is_null() {
                    return Ok("null".to_string());
                }
                Ok(serde_json::to_string(value)?)
            }
            Serializer::Json => {
                if value.is_null() {
                    return Ok(NIL_MARKER.to_string());
                }
                Ok(STANDARD.encode(serde_json::to_vec(value)?))
            }
        }
    }
}

/// Decode a stored TEXT value using the format recorded in its `serialization`
/// column. `None`, `""`, and `DBOS_JSON` all select the default (base64 JSON);
/// `portable_json` selects plain JSON. Any other name errors.
pub fn decode(format: Option<&str>, stored: &str) -> Result<Value> {
    match format.unwrap_or("") {
        PORTABLE => {
            if stored == "null" {
                return Ok(Value::Null);
            }
            Ok(serde_json::from_str(stored)?)
        }
        "" | DBOS_JSON => {
            if stored == NIL_MARKER {
                return Ok(Value::Null);
            }
            let bytes = STANDARD.decode(stored).map_err(|e| {
                Error::Serialization(format!("invalid base64 in {DBOS_JSON} value: {e}"))
            })?;
            Ok(serde_json::from_slice(&bytes)?)
        }
        other => Err(Error::Serialization(format!(
            "value uses serialization format {other:?}, which this SDK cannot decode; \
             use {PORTABLE} for cross-language interop"
        ))),
    }
}

/// Decode an optional stored value, defaulting absent/undecodable rows to `Null`
/// is *not* done here — callers that want lenient behavior handle the `Err`.
pub fn decode_opt(format: Option<&str>, stored: Option<&str>) -> Result<Option<Value>> {
    match stored {
        Some(s) => Ok(Some(decode(format, s)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_roundtrip_is_base64() {
        let v = json!({"a": 1, "b": "x"});
        let enc = Serializer::Json.encode(&v).unwrap();
        // base64, not plain JSON.
        assert!(!enc.starts_with('{'));
        assert_eq!(decode(Some(DBOS_JSON), &enc).unwrap(), v);
        // Empty/None format name falls back to DBOS_JSON.
        assert_eq!(decode(None, &enc).unwrap(), v);
    }

    #[test]
    fn portable_roundtrip_is_plain_json() {
        let v = json!({"a": 1});
        let enc = Serializer::Portable.encode(&v).unwrap();
        assert_eq!(enc, r#"{"a":1}"#);
        assert_eq!(decode(Some(PORTABLE), &enc).unwrap(), v);
    }

    #[test]
    fn nil_markers_roundtrip() {
        assert_eq!(Serializer::Json.encode(&Value::Null).unwrap(), NIL_MARKER);
        assert_eq!(Serializer::Portable.encode(&Value::Null).unwrap(), "null");
        assert_eq!(decode(Some(DBOS_JSON), NIL_MARKER).unwrap(), Value::Null);
        assert_eq!(decode(Some(PORTABLE), "null").unwrap(), Value::Null);
    }

    #[test]
    fn unknown_format_errors() {
        assert!(decode(Some("DBOS_PICKLE"), "abc").is_err());
        assert!(decode(Some("some_other_format"), "abc").is_err());
    }
}
