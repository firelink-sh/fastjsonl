use arrow::array::{
    BooleanBuilder, Float16Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, LargeStringBuilder,
};
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

fn append_json_value(builder: &mut Box<dyn ArrayBuilder>, dtype: &DataType, value: Option<&Value>) {
    match dtype {
        DataType::Int8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .expect("could not downcast builder to Int8Builder");
            match value {
                Some(v) => b.append_value(v.as_i64().unwrap() as i8),
                None => b.append_null(),
            }
        }
        DataType::Int16 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .expect("could not downcast builder to Int16Builder");
            match value {
                Some(v) => b.append_value(v.as_i64().unwrap() as i16),
                None => b.append_null(),
            }
        }
        DataType::Int32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .expect("could not downcast builder to Int32Builder");
            match value {
                Some(v) => b.append_value(v.as_i64().unwrap() as i32),
                None => b.append_null(),
            }
        }
        DataType::Int64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("could not downcast builder to Int64Builder");
            match value {
                Some(v) => b.append_value(v.as_i64().unwrap()),
                None => b.append_null(),
            }
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("could not downcast builder to StringBuilder");
            match value {
                Some(v) => {
                    if let Some(s) = v.as_str() {
                        b.append_value(s);
                    } else {
                        // If the json value is not actually a string, we can
                        // serialize it to utf8 using serde-json
                        b.append_value(serde_json::to_string(v).unwrap());
                    };
                }
                None => b.append_null(),
            }
        }
        DataType::LargeUtf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<LargeStringBuilder>()
                .expect("could not downcast builder to LargeStringBuilder");
            match value {
                Some(v) => {
                    if let Some(s) = v.as_str() {
                        b.append_value(s);
                    } else {
                        b.append_value(serde_json::to_string(v).unwrap());
                    };
                }
                None => b.append_null(),
            }
        }
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .expect("could not downcast builder to BooleanBuilder");
            match value {
                Some(v) => b.append_value(v.as_bool().unwrap()),
                None => b.append_null(),
            }
        }
        _ => {
            panic!("unsupported datatype: {:?}", dtype);
        }
    }
}

#[pyfunction]
fn jsonl_to_arrow(
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
    let mut builders: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int8 => Box::new(Int8Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int16 => Box::new(Int16Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int32 => Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Int64 => Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Float16 => Box::new(Float16Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Float32 => Box::new(Float32Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Float64 => Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            DataType::Utf8 => Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            DataType::LargeUtf8 => Box::new(LargeStringBuilder::new()) as Box<dyn ArrayBuilder>,
            DataType::Boolean => Box::new(BooleanBuilder::new()) as Box<dyn ArrayBuilder>,
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

        for (j, field) in schema.fields().iter().enumerate() {
            let key: &str = field.name();
            let value = json.get(key);
            // println!("row {} col {} '{}': '{:?}'", i, j, key, value);
            append_json_value(&mut builders[j], field.data_type(), value);
        }
    }

    // Finish the builders into recordbatch eatable arrays :) yummy!
    let columns: Vec<Arc<dyn Array>> = builders
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

    Ok(())
}

#[pymodule]
fn fastjsonl(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(validate_ndjson, m)?)?;
    m.add_function(wrap_pyfunction!(jsonl_to_arrow, m)?)?;
    Ok(())
}
