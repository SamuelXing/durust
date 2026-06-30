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
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;
use std::sync::Arc;

/// Cross-language wire format: plain JSON, readable by any DBOS SDK.
pub const PORTABLE: &str = "portable_json";
/// Default wire format: base64-encoded JSON.
pub const DBOS_JSON: &str = "DBOS_JSON";
/// Sentinel the default format writes for a nil value.
const NIL_MARKER: &str = "__DBOS_NIL";

/// A user-supplied serialization codec, plugged in via [`Serializer::custom`].
///
/// Implement this to store workflow data in a format other than the built-in
/// JSON variants — e.g. an encrypted, compressed, or binary encoding. The codec
/// works in JSON [`Value`] space: [`encode`](Self::encode) turns a value into the
/// TEXT written to the `serialization`-tagged column, and [`decode`](Self::decode)
/// reverses it. [`name`](Self::name) is the tag written alongside each value, so a
/// reader configured with the *same* codec can recognize and decode rows it wrote;
/// it must differ from the built-in `DBOS_JSON`/`portable_json` names.
pub trait SerializerCodec: Send + Sync {
    /// The format tag written to the `serialization` column for values this codec
    /// encodes. Used to route decoding back to this codec.
    fn name(&self) -> &str;
    /// Encode a JSON value to the TEXT stored in the database.
    fn encode(&self, value: &Value) -> Result<String>;
    /// Decode TEXT this codec previously wrote back into a JSON value.
    fn decode(&self, stored: &str) -> Result<Value>;
}

/// A serialization format for workflow data. Cheap to clone; held by each
/// provider as the format it *encodes* with (decoding is format-directed).
#[derive(Clone, Default)]
pub enum Serializer {
    /// `DBOS_JSON`: base64-encoded JSON. The default.
    #[default]
    Json,
    /// `portable_json`: plain JSON, readable across DBOS languages.
    Portable,
    /// A user-supplied [`SerializerCodec`], installed with [`Serializer::custom`].
    Custom(Arc<dyn SerializerCodec>),
}

impl fmt::Debug for Serializer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Serializer::Json => f.write_str("Json"),
            Serializer::Portable => f.write_str("Portable"),
            Serializer::Custom(c) => write!(f, "Custom({:?})", c.name()),
        }
    }
}

impl Serializer {
    /// Wrap a user-supplied [`SerializerCodec`] as a [`Serializer`]. Pass the
    /// result to a provider's `with_serializer` to store values in a custom format.
    pub fn custom(codec: Arc<dyn SerializerCodec>) -> Self {
        Serializer::Custom(codec)
    }

    /// The format name stored in the `serialization` column.
    pub fn name(&self) -> &str {
        match self {
            Serializer::Json => DBOS_JSON,
            Serializer::Portable => PORTABLE,
            Serializer::Custom(c) => c.name(),
        }
    }

    /// Encode a JSON value to its stored TEXT form.
    pub fn encode(&self, value: &Value) -> Result<String> {
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
            Serializer::Custom(c) => c.encode(value),
        }
    }
}

/// Decode a stored TEXT value using the format recorded in its `serialization`
/// column, falling back to `serializer` for any format the built-ins don't
/// recognize. `portable_json` always decodes as plain JSON and `None`/`""`/
/// `DBOS_JSON` as the default base64 JSON — regardless of `serializer`, so rows
/// written by another SDK still read. A format matching a configured custom
/// codec's name routes to that codec; any other name errors.
pub fn decode(serializer: &Serializer, format: Option<&str>, stored: &str) -> Result<Value> {
    match format.unwrap_or("") {
        PORTABLE => {
            if stored == "null" {
                return Ok(Value::Null);
            }
            Ok(serde_json::from_str(stored)?)
        }
        // A configured custom codec decodes the rows it wrote (checked before the
        // base64-JSON default so a codec may even reuse that name if it must).
        other if matches!(serializer, Serializer::Custom(c) if c.name() == other) => {
            match serializer {
                Serializer::Custom(c) => c.decode(stored),
                _ => unreachable!(),
            }
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
             use {PORTABLE} for cross-language interop, or configure a matching custom serializer"
        ))),
    }
}

/// Decode an optional stored value, defaulting absent/undecodable rows to `Null`
/// is *not* done here — callers that want lenient behavior handle the `Err`.
pub fn decode_opt(
    serializer: &Serializer,
    format: Option<&str>,
    stored: Option<&str>,
) -> Result<Option<Value>> {
    match stored {
        Some(s) => Ok(Some(decode(serializer, format, s)?)),
        None => Ok(None),
    }
}

/// The cross-language workflow-input envelope: `{"positionalArgs":[…],"namedArgs":{…}}`.
///
/// In [`Serializer::Portable`] mode a workflow's input is stored in this shape so
/// a DBOS app in another language (Go, Python, TypeScript, …) can run it: a Rust
/// workflow's single input becomes the one positional arg. Pass this type *as*
/// the input to target a workflow elsewhere that takes several positional or
/// named arguments (e.g. a Python `def wf(a, b, *, key)`); it is stored verbatim.
///
/// Field order matters: `positionalArgs` serializes first, then `namedArgs`
/// (always present, `{}` when empty) — the byte form Go and Python both emit.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PortableWorkflowArgs {
    #[serde(rename = "positionalArgs", default)]
    pub positional_args: Vec<Value>,
    #[serde(rename = "namedArgs", default)]
    pub named_args: Map<String, Value>,
}

impl PortableWorkflowArgs {
    /// Wrap a single value as the one positional arg (no named args) — how a
    /// one-input workflow's input is stored for cross-language execution.
    pub fn single(arg: Value) -> Self {
        Self {
            positional_args: vec![arg],
            named_args: Map::new(),
        }
    }
}

/// Read a value as the args envelope: it must be an object with an array
/// `positionalArgs`; `namedArgs` is optional (absent/null ⇒ empty). Anything
/// else is not an envelope (so a plain workflow input is wrapped, not mistaken
/// for one).
fn as_envelope(value: &Value) -> Option<PortableWorkflowArgs> {
    let obj = value.as_object()?;
    let positional_args = obj.get("positionalArgs")?.as_array()?.clone();
    let named_args = match obj.get("namedArgs") {
        Some(Value::Object(m)) => m.clone(),
        None | Some(Value::Null) => Map::new(),
        Some(_) => return None,
    };
    Some(PortableWorkflowArgs {
        positional_args,
        named_args,
    })
}

/// Encode a workflow **input** for storage. In [`Serializer::Portable`] mode the
/// value is wrapped in the cross-language args envelope (a plain value becomes
/// the single positional arg; a value already shaped like the envelope — e.g. a
/// [`PortableWorkflowArgs`] — is kept as-is). Other formats store it directly,
/// exactly like [`Serializer::encode`]. The envelope struct is serialized
/// directly so its bytes are `positionalArgs`-first.
pub fn encode_input(serializer: &Serializer, value: &Value) -> Result<String> {
    if matches!(serializer, Serializer::Portable) {
        let envelope =
            as_envelope(value).unwrap_or_else(|| PortableWorkflowArgs::single(value.clone()));
        return Ok(serde_json::to_string(&envelope)?);
    }
    serializer.encode(value)
}

/// Decode a stored workflow **input**. For a `portable_json` row the args
/// envelope is unwrapped to its first positional arg — what a single-input
/// workflow receives — tolerating a missing/empty `namedArgs` or `positionalArgs`
/// (⇒ `Null`). Other formats decode exactly like [`decode`].
pub fn decode_input(serializer: &Serializer, format: Option<&str>, stored: &str) -> Result<Value> {
    let value = decode(serializer, format, stored)?;
    if format == Some(PORTABLE) {
        Ok(first_positional(value))
    } else {
        Ok(value)
    }
}

/// `decode_input` over an optional column (mirrors [`decode_opt`]).
pub fn decode_input_opt(
    serializer: &Serializer,
    format: Option<&str>,
    stored: Option<&str>,
) -> Result<Option<Value>> {
    match stored {
        Some(s) => Ok(Some(decode_input(serializer, format, s)?)),
        None => Ok(None),
    }
}

/// The first positional arg of an args envelope (`Null` if empty); a value that
/// is not an envelope is returned unchanged.
fn first_positional(value: Value) -> Value {
    match as_envelope(&value) {
        Some(mut env) if !env.positional_args.is_empty() => env.positional_args.swap_remove(0),
        Some(_) => Value::Null,
        None => value,
    }
}

/// Generic name stored for an error that carries no cross-language type, written
/// when an untyped error is serialized in portable mode.
pub const PORTABLE_ERROR_NAME: &str = "Portable Error";

/// The cross-language workflow-**error** envelope: `{"name":…,"message":…,"code"?,"data"?}`.
///
/// In [`Serializer::Portable`] mode a failed workflow's stored error is written
/// in this shape so a DBOS app in another language can read it as a structured
/// error — a type/class name, the human message, and optional `code`/`data` —
/// rather than an opaque string. A native error carries no user-defined name, so
/// it is stored under the generic [`PORTABLE_ERROR_NAME`] with its display text as
/// `message`, and `code`/`data` are omitted when absent. The envelope is read
/// tolerantly: a value another SDK wrote — which may carry the concrete error
/// type's name and its own `code`/`data` — still decodes here.
///
/// This type is surfaced on [`crate::WorkflowStatus::error_info`] when reading a
/// portable error written by any SDK.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortableWorkflowError {
    pub name: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Encode a failed workflow's **error** for storage. In [`Serializer::Portable`]
/// mode it becomes the cross-language envelope: a structured [`Error::Portable`]
/// is written with the type name and `code`/`data` its author supplied; any other
/// error is wrapped under the generic [`PORTABLE_ERROR_NAME`] with its display
/// text as `message`. Other formats store the bare display text. This mirrors the
/// other SDKs' `serializeWorkflowError`, called at workflow completion — so only
/// the error outcome is encoded here; cancellation/timeout reasons are stored bare
/// by their own call sites.
pub fn encode_error(serializer: &Serializer, err: &Error) -> String {
    if matches!(serializer, Serializer::Portable) {
        let env = match err {
            Error::Portable(pe) => pe.clone(),
            other => PortableWorkflowError {
                name: PORTABLE_ERROR_NAME.to_string(),
                message: other.to_string(),
                code: None,
                data: None,
            },
        };
        // The envelope is plain JSON values, so serialization cannot fail; fall
        // back to the bare message in the impossible case that it does.
        return serde_json::to_string(&env).unwrap_or_else(|_| err.to_string());
    }
    err.to_string()
}

/// Decode a stored workflow **error**, returning its human message and — for a
/// `portable_json` row that holds a structured envelope — the full
/// [`PortableWorkflowError`] (name/code/data) another SDK wrote, or the generic
/// envelope Rust wrote. A non-portable row, or any value that is not a valid
/// envelope, decodes to a plain message with no structure, matching the other
/// SDKs (which only deserialize the envelope in portable mode and otherwise fall
/// back to the raw string).
pub fn decode_error(format: Option<&str>, stored: &str) -> (String, Option<PortableWorkflowError>) {
    if format == Some(PORTABLE) {
        if let Ok(env) = serde_json::from_str::<PortableWorkflowError>(stored) {
            return (env.message.clone(), Some(env));
        }
    }
    (stored.to_string(), None)
}

/// `decode_error` over an optional column: an absent error yields `(None, None)`.
pub fn decode_error_opt(
    format: Option<&str>,
    stored: Option<&str>,
) -> (Option<String>, Option<PortableWorkflowError>) {
    match stored {
        Some(s) => {
            let (msg, env) = decode_error(format, s);
            (Some(msg), env)
        }
        None => (None, None),
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
        assert_eq!(decode(&Serializer::Json, Some(DBOS_JSON), &enc).unwrap(), v);
        // Empty/None format name falls back to DBOS_JSON.
        assert_eq!(decode(&Serializer::Json, None, &enc).unwrap(), v);
    }

    #[test]
    fn portable_roundtrip_is_plain_json() {
        let v = json!({"a": 1});
        let enc = Serializer::Portable.encode(&v).unwrap();
        assert_eq!(enc, r#"{"a":1}"#);
        assert_eq!(decode(&Serializer::Json, Some(PORTABLE), &enc).unwrap(), v);
    }

    #[test]
    fn nil_markers_roundtrip() {
        assert_eq!(Serializer::Json.encode(&Value::Null).unwrap(), NIL_MARKER);
        assert_eq!(Serializer::Portable.encode(&Value::Null).unwrap(), "null");
        assert_eq!(
            decode(&Serializer::Json, Some(DBOS_JSON), NIL_MARKER).unwrap(),
            Value::Null
        );
        assert_eq!(
            decode(&Serializer::Json, Some(PORTABLE), "null").unwrap(),
            Value::Null
        );
    }

    #[test]
    fn unknown_format_errors() {
        assert!(decode(&Serializer::Json, Some("DBOS_PICKLE"), "abc").is_err());
        assert!(decode(&Serializer::Json, Some("some_other_format"), "abc").is_err());
    }

    #[test]
    fn portable_input_wraps_single_value() {
        // A plain workflow input becomes the single positional arg —
        // positionalArgs first, then namedArgs (`{}` when empty).
        let enc = encode_input(&Serializer::Portable, &json!("hello")).unwrap();
        assert_eq!(enc, r#"{"positionalArgs":["hello"],"namedArgs":{}}"#);
        // Decoding returns the bare value a single-input workflow receives.
        assert_eq!(
            decode_input(&Serializer::Json, Some(PORTABLE), &enc).unwrap(),
            json!("hello")
        );
    }

    #[test]
    fn portable_input_matches_cross_language_canonical() {
        // The canonical interop input set, wrapped as positional args, must
        // produce the exact cross-language bytes (positionalArgs first).
        let args = PortableWorkflowArgs {
            positional_args: vec![
                json!("hello-interop"),
                json!(42),
                json!("2025-06-15T10:30:00.000Z"),
                json!(["alpha", "beta", "gamma"]),
                json!({"key1": "value1", "key2": 99, "nested": {"deep": true}}),
                json!(true),
                json!(null),
            ],
            named_args: Map::new(),
        };
        let enc =
            encode_input(&Serializer::Portable, &serde_json::to_value(&args).unwrap()).unwrap();
        assert_eq!(
            enc,
            r#"{"positionalArgs":["hello-interop",42,"2025-06-15T10:30:00.000Z",["alpha","beta","gamma"],{"key1":"value1","key2":99,"nested":{"deep":true}},true,null],"namedArgs":{}}"#
        );
    }

    #[test]
    fn portable_input_keeps_explicit_named_args() {
        // Passing PortableWorkflowArgs (e.g. to call a Python `def wf(a, *, name)`)
        // is normalized to positionalArgs-first and stored verbatim, not re-wrapped.
        let mut named = Map::new();
        named.insert("name".to_string(), json!("test"));
        let args = PortableWorkflowArgs {
            positional_args: vec![json!(1)],
            named_args: named,
        };
        let enc =
            encode_input(&Serializer::Portable, &serde_json::to_value(&args).unwrap()).unwrap();
        assert_eq!(enc, r#"{"positionalArgs":[1],"namedArgs":{"name":"test"}}"#);
    }

    #[test]
    fn portable_input_reads_either_field_order() {
        // Readers parse by key, so a producer that writes namedArgs first (Python)
        // decodes the same as positionalArgs first (Go).
        for stored in [
            r#"{"positionalArgs":["x"],"namedArgs":{}}"#,
            r#"{"namedArgs":{},"positionalArgs":["x"]}"#,
            r#"{"positionalArgs":["x"]}"#, // minimal: no namedArgs
        ] {
            assert_eq!(
                decode_input(&Serializer::Json, Some(PORTABLE), stored).unwrap(),
                json!("x")
            );
        }
        // Empty positional args ⇒ null (a no-arg call).
        assert_eq!(
            decode_input(
                &Serializer::Json,
                Some(PORTABLE),
                r#"{"positionalArgs":[],"namedArgs":{}}"#
            )
            .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn portable_error_wraps_under_generic_name() {
        // An untyped error becomes the cross-language envelope under the generic
        // name — message present, code/data omitted (matching Go and Python bytes).
        let enc = encode_error(&Serializer::Portable, &Error::app("boom"));
        assert_eq!(enc, r#"{"name":"Portable Error","message":"boom"}"#);
        // It decodes back to the human message plus the structured envelope.
        let (msg, info) = decode_error(Some(PORTABLE), &enc);
        assert_eq!(msg, "boom");
        let info = info.expect("portable error decodes to a structured envelope");
        assert_eq!(info.name, PORTABLE_ERROR_NAME);
        assert_eq!(info.message, "boom");
        assert!(info.code.is_none() && info.data.is_none());
    }

    #[test]
    fn portable_error_keeps_typed_name_and_code() {
        // A structured Error::Portable is written with its own name/code/data —
        // and read back intact, the bytes any SDK can parse.
        let err = Error::Portable(PortableWorkflowError {
            name: "ValidationError".to_string(),
            message: "invalid input".to_string(),
            code: Some(json!(400)),
            data: Some(json!({"field": "email"})),
        });
        let enc = encode_error(&Serializer::Portable, &err);
        let (msg, info) = decode_error(Some(PORTABLE), &enc);
        assert_eq!(msg, "invalid input");
        let info = info.expect("typed portable error decodes");
        assert_eq!(info.name, "ValidationError");
        assert_eq!(info.code, Some(json!(400)));
        assert_eq!(info.data, Some(json!({"field": "email"})));
    }

    #[test]
    fn json_error_stays_bare() {
        // Default (non-portable) mode stores and reads the plain message — no
        // envelope, no structured info — even for a typed error.
        assert_eq!(encode_error(&Serializer::Json, &Error::app("boom")), "boom");
        assert_eq!(
            encode_error(&Serializer::Json, &Error::portable("Validation", "boom")),
            "boom"
        );
        assert_eq!(
            decode_error(Some(DBOS_JSON), "boom"),
            ("boom".to_string(), None)
        );
        assert_eq!(decode_error(None, "boom"), ("boom".to_string(), None));
    }

    #[test]
    fn portable_error_reads_another_sdk_structured_error() {
        // A structured error written elsewhere (a real name + numeric code +
        // data) surfaces with all fields intact.
        let stored = r#"{"name":"ValidationError","message":"invalid input","code":400,"data":{"field":"email"}}"#;
        let (msg, info) = decode_error(Some(PORTABLE), stored);
        assert_eq!(msg, "invalid input");
        let info = info.expect("structured envelope decodes");
        assert_eq!(info.name, "ValidationError");
        assert_eq!(info.code, Some(json!(400)));
        assert_eq!(info.data, Some(json!({"field": "email"})));
    }

    #[test]
    fn portable_error_falls_back_on_non_envelope() {
        // A portable row that somehow holds a bare (non-JSON) string still reads
        // as a plain message — never a parse error — like the other SDKs.
        assert_eq!(
            decode_error(Some(PORTABLE), "just a string"),
            ("just a string".to_string(), None)
        );
        // decode_error_opt threads None through as no error.
        assert_eq!(decode_error_opt(Some(PORTABLE), None), (None, None));
    }

    #[test]
    fn json_input_is_not_enveloped() {
        // Default (non-portable) mode stores the input directly — no envelope —
        // and decodes it back unchanged.
        let enc = encode_input(&Serializer::Json, &json!("hello")).unwrap();
        assert_eq!(enc, Serializer::Json.encode(&json!("hello")).unwrap());
        assert_eq!(
            decode_input(&Serializer::Json, Some(DBOS_JSON), &enc).unwrap(),
            json!("hello")
        );
        // An envelope-shaped value is left intact in JSON mode (never unwrapped).
        let shaped = json!({"positionalArgs": ["x"], "namedArgs": {}});
        let enc2 = encode_input(&Serializer::Json, &shaped).unwrap();
        assert_eq!(
            decode_input(&Serializer::Json, Some(DBOS_JSON), &enc2).unwrap(),
            shaped
        );
    }
}
