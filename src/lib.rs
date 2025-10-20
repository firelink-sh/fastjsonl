use arrow_array::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use jsonschema;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyRecordBatch, PySchema};
use serde_json::Value;
use std::io::{BufRead, BufReader};

#[pyfunction]
fn test(
    py: Python,
    buffer: &Bound<PyBytes>,
    json_schema_str: &Bound<PyString>,
    arrow_schema: PySchema,
) -> PyArrowResult<PyRecordBatch> {
    println!("ARROW SCHEMA FROM PYTHON: {:?}", arrow_schema);

    let rb = RecordBatch::new_empty(arrow_schema.into());
    Ok(rb.into())
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
