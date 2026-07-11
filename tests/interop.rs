//! Cross-SDK portable-serialization conformance.
//!
//! The DBOS SDKs interoperate on one Postgres database, so a value one SDK
//! writes in `portable_json` must be *byte-identical* to what the others write
//! for the same logical data. These golden strings are the cross-language
//! contract — they are pinned identically in the Python, TypeScript, Java, and
//! Go SDK test suites (see `dbos-transact-py/tests/test_interop.py`,
//! `dbos-transact-golang/dbos/serialization_test.go`). durare must reproduce
//! them exactly.

use durare::{PortableWorkflowArgs, PortableWorkflowError, Serializer};
use serde::Serialize;
use serde_json::{json, Value};

// ---- Golden strings (byte-identical across dbos-transact-{py,ts,java,go}) ----

const GOLDEN_OUTPUT_JSON: &str = r#"{"echo_text":"hello-interop","echo_num":42,"echo_dt":"2025-06-15T10:30:00.000Z","items_count":3,"meta_keys":["key1","key2","nested"],"flag":true,"empty":null,"received":{"sender":"test","payload":[1,2,3]}}"#;
const GOLDEN_EVENT_JSON: &str = r#"{"text":"hello-interop","num":42,"flag":true}"#;
const GOLDEN_STREAM_JSON: &str = r#"{"item":"hello-interop"}"#;
const GOLDEN_MESSAGE_JSON: &str = r#"{"sender":"test","payload":[1,2,3]}"#;

/// Encode a value exactly as durare does when checkpointing it in portable mode:
/// the workflow's typed return is turned into a `serde_json::Value`, then the
/// portable serializer stringifies it.
fn portable_encode<T: Serialize>(value: &T) -> String {
    let v = serde_json::to_value(value).unwrap();
    Serializer::Portable.encode(&v).unwrap()
}

#[derive(Serialize)]
struct Received {
    sender: String,
    payload: Vec<i64>,
}

#[derive(Serialize)]
struct CanonicalOutput {
    echo_text: String,
    echo_num: i64,
    echo_dt: String,
    items_count: i64,
    meta_keys: Vec<String>,
    flag: bool,
    empty: Option<()>,
    received: Received,
}

#[derive(Serialize)]
struct CanonicalEvent {
    text: String,
    num: i64,
    flag: bool,
}

#[derive(Serialize)]
struct CanonicalStream {
    item: String,
}

fn canonical_output() -> CanonicalOutput {
    CanonicalOutput {
        echo_text: "hello-interop".into(),
        echo_num: 42,
        echo_dt: "2025-06-15T10:30:00.000Z".into(),
        items_count: 3,
        meta_keys: vec!["key1".into(), "key2".into(), "nested".into()],
        flag: true,
        empty: None,
        received: Received {
            sender: "test".into(),
            payload: vec![1, 2, 3],
        },
    }
}

#[test]
fn portable_output_matches_golden() {
    assert_eq!(portable_encode(&canonical_output()), GOLDEN_OUTPUT_JSON);
}

#[test]
fn portable_event_matches_golden() {
    let event = CanonicalEvent {
        text: "hello-interop".into(),
        num: 42,
        flag: true,
    };
    assert_eq!(portable_encode(&event), GOLDEN_EVENT_JSON);
}

#[test]
fn portable_stream_value_matches_golden() {
    let value = CanonicalStream {
        item: "hello-interop".into(),
    };
    assert_eq!(portable_encode(&value), GOLDEN_STREAM_JSON);
}

#[test]
fn portable_message_matches_golden() {
    let message = Received {
        sender: "test".into(),
        payload: vec![1, 2, 3],
    };
    assert_eq!(portable_encode(&message), GOLDEN_MESSAGE_JSON);
}

#[test]
fn portable_error_envelope_matches_golden() {
    // A structured error with a name + message, no code/data, encodes to the
    // cross-language envelope with fields in declaration order and code/data
    // omitted when absent.
    let err = PortableWorkflowError {
        name: "ValidationError".into(),
        message: "amount must be positive".into(),
        code: None,
        data: None,
    };
    assert_eq!(
        serde_json::to_string(&err).unwrap(),
        r#"{"name":"ValidationError","message":"amount must be positive"}"#
    );
}

// ---- Read direction: durare must decode what the other SDKs write ----

const GOLDEN_INPUTS_JSON: &str = r#"{"positionalArgs":["hello-interop",42,"2025-06-15T10:30:00.000Z",["alpha","beta","gamma"],{"key1":"value1","key2":99,"nested":{"deep":true}},true,null]}"#;

#[test]
fn decodes_golden_input_envelope() {
    // The positionalArgs-only form (TS/Java/Go golden; Python's namedArgs prefix
    // stripped): durare reads all seven positional args, no named args.
    let args: PortableWorkflowArgs = serde_json::from_str(GOLDEN_INPUTS_JSON).unwrap();
    assert_eq!(args.positional_args.len(), 7);
    assert_eq!(args.positional_args[0], json!("hello-interop"));
    assert_eq!(args.positional_args[1], json!(42));
    assert_eq!(args.positional_args[6], Value::Null);
    assert!(args.named_args.is_empty());
}

#[test]
fn decodes_python_namedargs_first_input() {
    // Python writes `{"namedArgs":{},"positionalArgs":[...]}` (namedArgs first).
    // JSON is order-insensitive on read, so durare decodes it to the same args.
    let python_form = r#"{"namedArgs":{"limit":10},"positionalArgs":["hello-interop",42]}"#;
    let args: PortableWorkflowArgs = serde_json::from_str(python_form).unwrap();
    assert_eq!(
        args.positional_args,
        vec![json!("hello-interop"), json!(42)]
    );
    assert_eq!(args.named_args.get("limit"), Some(&json!(10)));
}

#[test]
fn durare_input_envelope_is_positionalargs_first() {
    // durare's own write form matches the TS/Java/Go ordering: positionalArgs
    // first, with namedArgs always present (empty when there are none).
    let args = PortableWorkflowArgs {
        positional_args: vec![json!("hello"), json!(42)],
        named_args: Default::default(),
    };
    assert_eq!(
        serde_json::to_string(&args).unwrap(),
        r#"{"positionalArgs":["hello",42],"namedArgs":{}}"#
    );
}

#[test]
fn decodes_foreign_structured_error() {
    // A structured error another SDK wrote, carrying a concrete type name and a
    // numeric code, round-trips into durare's PortableWorkflowError.
    let foreign = r#"{"name":"InsufficientFunds","message":"balance too low","code":402,"data":{"available":10}}"#;
    let err: PortableWorkflowError = serde_json::from_str(foreign).unwrap();
    assert_eq!(err.name, "InsufficientFunds");
    assert_eq!(err.code, Some(json!(402)));
    assert_eq!(err.data, Some(json!({"available": 10})));
}

// ---- Round-trip fidelity: read a foreign portable value, re-write it byte-identically ----

#[test]
fn portable_values_round_trip_byte_identically() {
    // Reading another SDK's portable record and re-writing it (e.g. on
    // export/import or a passthrough step) must not perturb a single byte.
    for golden in [
        GOLDEN_OUTPUT_JSON,
        GOLDEN_EVENT_JSON,
        GOLDEN_STREAM_JSON,
        GOLDEN_MESSAGE_JSON,
    ] {
        let value: Value = serde_json::from_str(golden).unwrap();
        assert_eq!(Serializer::Portable.encode(&value).unwrap(), golden);
    }
}
