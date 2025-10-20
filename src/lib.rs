use arrow::array::{Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeStringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use arrow_array::{Array, RecordBatch};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use jsonschema;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyRecordBatch, PySchema};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

#[pyfunction]
fn test(
    buffer: &Bound<PyBytes>,
    json_schema_str: &Bound<PyString>,
    pyarrow_schema: PySchema,
) -> PyArrowResult<PyRecordBatch> {
    let json_schema: Value = serde_json::from_str(json_schema_str.to_str()?).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid jsonschema: {}", e))
    })?;

    let validator = jsonschema::draft202012::new(&json_schema).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "jsonschema is not valid draft202012: {}",
            e
        ))
    })?;

    let schema: SchemaRef = pyarrow_schema.into_inner();
    let mut column_builders: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int8 => Box::new(Int8Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int16 => Box::new(Int16Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int32 => Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int64 => Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Utf8 => Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            DataType::LargeUtf8 => Box::new(LargeStringBuilder::new()) as Box<dyn ArrayBuilder>,
            dtype => panic!("unsupported datatype: {:?}", dtype),
        })
        .collect();

    let reader = BufReader::new(buffer.as_bytes());
    for (i, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => panic!("buffered read error: {}", e),
        };

        let json: Value = match serde_json::from_str(&line) {
            Ok(val) => val,
            Err(e) => panic!("parse error: {}", e),
        };

        if let Err(e) = validator.validate(&json) {
            panic!("validation error (row {}): {}", i, e)
        };

        println!("LINE: {}", line);
        println!("JSON: {}", json);

        for (j, field) in schema.fields().iter().enumerate() {
            let key: &str = field.name();
            let value = json.get(key);

            println!("{}: {}", key, value.unwrap());

            match (column_builders[j].as_any_mut(), field.data_type(), value) {
                (builder, DataType::Int64, Some(v)) => {
                    println!("row={} col={} BUILDER: {:?}", i, j, builder);
                    let b = builder
                        .downcast_mut::<Int64Builder>()
                        .expect("could not downcast builder to in64");
                    if let Some(n) = v.as_i64() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                }
                (builder, DataType::Utf8, Some(v)) => {
                    println!("row={} col={} BUILDER: {:?}", i, j, builder);
                    let b = builder
                        .downcast_mut::<StringBuilder>()
                        .expect("could not downcast builder to string");
                    if let Some(n) = v.as_str() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                }
                (builder, DataType::LargeUtf8, Some(v)) => {
                    println!("row={} col={} BUILDER: {:?}", i, j, builder);
                    let b = builder
                        .downcast_mut::<LargeStringBuilder>()
                        .expect("could not downcast builder to LargeString");
                    if let Some(n) = v.as_str() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                }
                (_, _, None) => {}
                _ => panic!("type mismatch for field '{}'", field),
            };
        }
    }

    // Finish the builders into recordbatch eatable arrays :) yummy!
    let columns: Vec<Arc<dyn Array>> = column_builders
        .into_iter()
        .map(|mut cb| cb.finish())
        .map(Arc::from)
        .collect();

    let record_batch = RecordBatch::try_new(schema, columns)?;
    Ok(record_batch.into())
}

#[pyfunction]
fn validate_ndjson(buffer: &Bound<PyBytes>, schema_str: &Bound<PyString>) -> PyResult<()> {
    validate_jsonl(buffer, schema_str)
}

#[pyfunction]
fn validate_jsonl(buffer: &Bound<PyBytes>, schema_str: &Bound<PyString>) -> PyResult<()> {
    let json_schema: Value = serde_json::from_str(schema_str.to_str()?).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid jsonschema: {}", e))
    })?;

    let validator = jsonschema::draft202012::new(&json_schema).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "jsonschema is not valid draft202012: {}",
            e
        ))
    })?;

    let reader = BufReader::new(buffer.as_bytes());
    for (i, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                return Err(pyo3::exceptions::PyIOError::new_err(format!(
                    "buffered read error: {}",
                    e
                )));
            }
        };

        let json: Value = match serde_json::from_str(&line) {
            Ok(val) => val,
            Err(e) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Parse error: {}",
                    e
                )));
            }
        };

        if let Err(e) = validator.validate(&json) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Validation error (row={}): {}",
                i, e,
            )));
        }
    }

    drop(json_schema);
    drop(validator);

    Ok(())
}

#[pymodule]
fn fastjsonl(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(validate_ndjson, m)?)?;
    m.add_function(wrap_pyfunction!(test, m)?)?;
    Ok(())
}
