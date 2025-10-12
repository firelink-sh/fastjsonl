use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use serde_json::Value;

#[pyfunction]
fn count_newlines(data: &Bound<PyBytes>) -> PyResult<usize> {
    let slice: &[u8] = data.as_bytes();
    let count = slice.iter().filter(|&&b| b == b'\n').count();
    Ok(count)
}

#[pyfunction]
fn greet(name: &str) -> PyResult<String> {
    Ok(format!("Hello {} from Rust!", name))
}

#[pyfunction]
fn validate_jsonl(data: &Bound<PyBytes>, schema_str: &Bound<PyString>) -> PyResult<()> {
    let json_schema: Value = serde_json::from_str(schema_str.to_str()?).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid jsonschema: {}", e))
    })?;
    println!("schema: {}", json_schema);
    Ok(())
}

#[pymodule]
fn fastjsonl_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(greet, m)?)?;
    m.add_function(wrap_pyfunction!(count_newlines, m)?)?;
    m.add_function(wrap_pyfunction!(validate_jsonl, m)?)?;
    Ok(())
}
