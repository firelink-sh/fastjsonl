use pyo3::prelude::*;
use pyo3::types::PyBytes;

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

#[pymodule]
fn fastjsonl_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(greet, m)?)?;
    m.add_function(wrap_pyfunction!(count_newlines, m)?)?;
    Ok(())
}
