use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;

pub fn jsonl_to_arrow(bytes: &[u8], schema_str: &str) -> RecordBatch {
    let json_schema: Value = serde_json::from_str(schema_str).expect("Could not parse json schema");
    todo!()
}
