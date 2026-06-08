use crate::storage::StorageEngine;
use crate::types::Query;
use crate::types::{AggregateStrategy, DataValue, FilterExpr, Op, PipelineStage, Record};
use futures_util::stream::{self, Stream, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Dynamic field extraction from DataValue (supports JSON in DataValue::String and MessagePack in DataValue::Binary)
pub fn extract_field(value: &DataValue, field: &str) -> Option<DataValue> {
    match value {
        DataValue::String(s) => {
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(s)
                && let Some(field_val) = json_val.get(field)
            {
                return match field_val {
                    serde_json::Value::Null => Some(DataValue::Null),
                    serde_json::Value::Bool(b) => Some(DataValue::Bool(*b)),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Some(DataValue::Int(i))
                        } else if let Some(u) = n.as_u64() {
                            Some(DataValue::UInt(u))
                        } else {
                            n.as_f64()
                                .map(|f| DataValue::Float(ordered_float::OrderedFloat(f)))
                        }
                    }
                    serde_json::Value::String(str_val) => Some(DataValue::String(str_val.clone())),
                    _ => Some(DataValue::String(field_val.to_string())),
                };
            }
        }
        DataValue::Binary(b) => {
            if let Ok(msg_val) = rmp_serde::from_slice::<serde_json::Value>(b)
                && let Some(field_val) = msg_val.get(field)
            {
                return match field_val {
                    serde_json::Value::Null => Some(DataValue::Null),
                    serde_json::Value::Bool(b) => Some(DataValue::Bool(*b)),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Some(DataValue::Int(i))
                        } else if let Some(u) = n.as_u64() {
                            Some(DataValue::UInt(u))
                        } else {
                            n.as_f64()
                                .map(|f| DataValue::Float(ordered_float::OrderedFloat(f)))
                        }
                    }
                    serde_json::Value::String(str_val) => Some(DataValue::String(str_val.clone())),
                    _ => Some(DataValue::String(field_val.to_string())),
                };
            }
        }
        _ => {}
    }
    None
}

/// Evaluates a field on a Record, supporting both system fields and nested payload fields.
pub fn evaluate_record_field(record: &Record, field: &str) -> Option<DataValue> {
    match field {
        "key" => Some(DataValue::String(record.key.to_string())),
        "timestamp" => Some(DataValue::Int(record.timestamp)),
        "sequence_id" => Some(DataValue::UInt(record.sequence_id)),
        "stream" => Some(DataValue::String(record.stream_name.clone())),
        "value" => Some(record.value.clone()),
        _ => extract_field(&record.value, field),
    }
}

/// Evaluates recursive FilterExpr boolean trees.
pub fn evaluate_filter_expr(record: &Record, expr: &FilterExpr) -> bool {
    match expr {
        FilterExpr::Simple {
            field,
            operator,
            value,
        } => {
            if let Some(actual_val) = evaluate_record_field(record, field) {
                compare_values(&actual_val, *operator, value)
            } else {
                false
            }
        }
        FilterExpr::And { left, right } => {
            evaluate_filter_expr(record, left) && evaluate_filter_expr(record, right)
        }
        FilterExpr::Or { left, right } => {
            evaluate_filter_expr(record, left) || evaluate_filter_expr(record, right)
        }
    }
}

/// Generic converter from DataValue to serde_json::Value
pub fn datavalue_to_json(val: &DataValue) -> serde_json::Value {
    match val {
        DataValue::Null => serde_json::Value::Null,
        DataValue::Bool(b) => serde_json::Value::Bool(*b),
        DataValue::Int(i) => serde_json::Value::Number((*i).into()),
        DataValue::UInt(u) => serde_json::Value::Number((*u).into()),
        DataValue::Float(f) => serde_json::Number::from_f64(f.into_inner())
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        DataValue::String(s) => {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                parsed
            } else {
                serde_json::Value::String(s.clone())
            }
        }
        DataValue::Binary(b) => {
            if let Ok(msg_val) = rmp_serde::from_slice::<serde_json::Value>(b) {
                msg_val
            } else {
                serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
            }
        }
        DataValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(datavalue_to_json).collect())
        }
        DataValue::Vector(vec) => serde_json::Value::Array(
            vec.iter()
                .map(|&x| serde_json::Value::Number(x.into()))
                .collect(),
        ),
    }
}

/// Evaluates comparisons with cross-type numeric coercion, startsWith, and set-membership (in).
pub fn compare_values(a: &DataValue, op: Op, b: &DataValue) -> bool {
    match op {
        Op::In => {
            if let DataValue::Array(arr) = b {
                arr.iter().any(|item| {
                    if let (Some(a_num), Some(item_num)) = (to_f64(a), to_f64(item)) {
                        ordered_float::OrderedFloat(a_num) == ordered_float::OrderedFloat(item_num)
                    } else {
                        a == item
                    }
                })
            } else {
                false
            }
        }
        Op::StartsWith => {
            if let (DataValue::String(s_a), DataValue::String(s_b)) = (a, b) {
                s_a.starts_with(s_b)
            } else {
                false
            }
        }
        _ => {
            if let (Some(a_num), Some(b_num)) = (to_f64(a), to_f64(b)) {
                let a_ord = ordered_float::OrderedFloat(a_num);
                let b_ord = ordered_float::OrderedFloat(b_num);
                match op {
                    Op::Eq => a_ord == b_ord,
                    Op::NotEq => a_ord != b_ord,
                    Op::Gt => a_ord > b_ord,
                    Op::Lt => a_ord < b_ord,
                    Op::GtEq => a_ord >= b_ord,
                    Op::LtEq => a_ord <= b_ord,
                    _ => false,
                }
            } else {
                match op {
                    Op::Eq => a == b,
                    Op::NotEq => a != b,
                    Op::Gt => a > b,
                    Op::Lt => a < b,
                    Op::GtEq => a >= b,
                    Op::LtEq => a <= b,
                    _ => false,
                }
            }
        }
    }
}

fn to_f64(val: &DataValue) -> Option<f64> {
    match val {
        DataValue::Int(i) => Some(*i as f64),
        DataValue::UInt(u) => Some(*u as f64),
        DataValue::Float(f) => Some(f.into_inner()),
        DataValue::Array(_) => None,
        _ => None,
    }
}

pub fn to_vector(val: &DataValue) -> Option<Vec<i8>> {
    match val {
        DataValue::Vector(v) => Some(v.clone()),
        DataValue::Array(arr) => {
            let mut vec = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    DataValue::Int(i) => vec.push(*i as i8),
                    DataValue::UInt(u) => vec.push(*u as i8),
                    DataValue::Float(f) => vec.push(f.into_inner() as i8),
                    _ => return None,
                }
            }
            Some(vec)
        }
        _ => None,
    }
}

pub fn cosine_similarity(a: &[i8], b: &[i8]) -> Option<f64> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let mut dot_product: i64 = 0;
    let mut norm_a: i64 = 0;
    let mut norm_b: i64 = 0;

    for (&x, &y) in a.iter().zip(b.iter()) {
        let x_i = x as i64;
        let y_i = y as i64;
        dot_product += x_i * y_i;
        norm_a += x_i * x_i;
        norm_b += y_i * y_i;
    }

    if norm_a == 0 || norm_b == 0 {
        return Some(0.0);
    }

    let sim = (dot_product as f64) / ((norm_a as f64).sqrt() * (norm_b as f64).sqrt());
    Some(sim)
}

/// Dynamic projection mapping for columns.
pub fn project_record(record: &mut Record, projections: &[String]) {
    let mut obj = serde_json::Map::new();

    // Helper to insert field into json map
    let insert_val = |obj: &mut serde_json::Map<String, serde_json::Value>,
                      key_str: &str,
                      dataval: &DataValue| {
        obj.insert(key_str.to_string(), datavalue_to_json(dataval));
    };

    // If source is a JSON-like string, we can project keys from it
    let source_json = match &record.value {
        DataValue::String(s) => serde_json::from_str::<serde_json::Value>(s).ok(),
        _ => None,
    };

    for proj in projections {
        if proj == "key" {
            obj.insert(
                "key".to_string(),
                serde_json::Value::String(record.key.to_string()),
            );
        } else if proj == "timestamp" {
            obj.insert(
                "timestamp".to_string(),
                serde_json::Value::Number(record.timestamp.into()),
            );
        } else if proj == "sequence_id" {
            obj.insert(
                "sequence_id".to_string(),
                serde_json::Value::Number(record.sequence_id.into()),
            );
        } else if proj == "stream" {
            obj.insert(
                "stream".to_string(),
                serde_json::Value::String(record.stream_name.clone()),
            );
        } else if let Some(json) = &source_json {
            if let Some(val) = json.get(proj) {
                obj.insert(proj.clone(), val.clone());
            }
        } else if proj == "value" {
            insert_val(&mut obj, "value", &record.value);
        }
    }

    record.value = DataValue::String(serde_json::Value::Object(obj).to_string());
}

/// Performs lazy streaming enrichment lookup.
pub fn enrich_record(
    record: &mut Record,
    engine: &StorageEngine,
    source_stream: &str,
    join_key: &str,
) {
    if let Some(join_val) = evaluate_record_field(record, join_key) {
        let lookup_key = match join_val {
            DataValue::String(s) => s,
            DataValue::Int(i) => i.to_string(),
            DataValue::UInt(u) => u.to_string(),
            _ => format!("{:?}", join_val),
        };

        if let Ok(Some(other_record)) = engine.get(source_stream, &lookup_key) {
            let mut main_obj = match &record.value {
                DataValue::String(s) => {
                    serde_json::from_str::<serde_json::Value>(s).unwrap_or(serde_json::Value::Null)
                }
                _ => serde_json::Value::Null,
            };

            let other_obj = match &other_record.value {
                DataValue::String(s) => {
                    serde_json::from_str::<serde_json::Value>(s).unwrap_or(serde_json::Value::Null)
                }
                _ => serde_json::Value::Null,
            };

            if let (Some(m_map), Some(o_map)) = (main_obj.as_object_mut(), other_obj.as_object()) {
                for (k, v) in o_map {
                    m_map.insert(k.clone(), v.clone());
                }
                record.value =
                    DataValue::String(serde_json::Value::Object(m_map.clone()).to_string());
            } else if main_obj.is_null() && other_obj.is_object() {
                record.value = other_record.value;
            }
        }
    }
}

fn extract_unique_key_from_pipeline(stages: &[PipelineStage]) -> Option<String> {
    for stage in stages {
        match stage {
            PipelineStage::Get { key } => {
                return Some(key.clone());
            }
            PipelineStage::Filter { expr } => {
                if let Some(key) = extract_key_from_filter(expr) {
                    return Some(key);
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_key_from_filter(expr: &FilterExpr) -> Option<String> {
    match expr {
        FilterExpr::Simple {
            field,
            operator,
            value,
        } => {
            if field == "key"
                && *operator == Op::Eq
                && let DataValue::String(k) = value
            {
                return Some(k.clone());
            }
            None
        }
        FilterExpr::And { left, right } => {
            extract_key_from_filter(left).or_else(|| extract_key_from_filter(right))
        }
        _ => None,
    }
}

/// Converts a JSON Value into its corresponding DataValue type.
pub fn json_to_datavalue(val: serde_json::Value) -> DataValue {
    match val {
        serde_json::Value::Null => DataValue::Null,
        serde_json::Value::Bool(b) => DataValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DataValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                DataValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                DataValue::Float(ordered_float::OrderedFloat(f))
            } else {
                DataValue::Null
            }
        }
        serde_json::Value::String(s) => DataValue::String(s),
        serde_json::Value::Array(arr) => {
            DataValue::Array(arr.into_iter().map(json_to_datavalue).collect())
        }
        serde_json::Value::Object(obj) => {
            DataValue::String(serde_json::Value::Object(obj).to_string())
        }
    }
}

/// Executes any full parsed Query on the StorageEngine, returning results or execution errors.
pub fn execute_query(engine: &StorageEngine, query: &Query) -> Result<Vec<Record>, String> {
    match query {
        Query::Pipeline(stages) => {
            if let Some(PipelineStage::From { stream_name }) = stages.first()
                && !engine.list_streams().contains(stream_name)
            {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }
            let mut records = engine.scan_historical()?;
            apply_pipeline_stages_to_vec(&mut records, engine, stages);
            Ok(records)
        }
        Query::Insert {
            stream_name,
            key,
            value,
        } => {
            if engine.get(stream_name, key)?.is_some() {
                return Err(format!(
                    "Key '{}' already exists in stream '{}'",
                    key, stream_name
                ));
            }
            let record =
                engine.append(stream_name, key, json_to_datavalue(value.clone()), false)?;
            Ok(vec![record])
        }
        Query::InsertBatch { stream_name, batch } => {
            // Verify none of the keys in the batch exist
            for (key, _) in batch {
                if engine.get(stream_name, key)?.is_some() {
                    return Err(format!(
                        "Key '{}' already exists in stream '{}' (batch insertion aborted)",
                        key, stream_name
                    ));
                }
            }
            let mut inserted = Vec::new();
            for (key, val) in batch {
                let record =
                    engine.append(stream_name, key, json_to_datavalue(val.clone()), false)?;
                inserted.push(record);
            }
            Ok(inserted)
        }
        Query::Upsert {
            stream_name,
            key,
            value,
        } => {
            let record =
                engine.append(stream_name, key, json_to_datavalue(value.clone()), false)?;
            Ok(vec![record])
        }
        Query::UpsertBatch { stream_name, batch } => {
            let mut inserted = Vec::new();
            for (key, val) in batch {
                let record =
                    engine.append(stream_name, key, json_to_datavalue(val.clone()), false)?;
                inserted.push(record);
            }
            Ok(inserted)
        }
        Query::Update {
            stream_name,
            key,
            value,
        } => {
            if !engine.list_streams().contains(stream_name) {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }
            let mut affected_rows = 0;
            if let Some(existing) = engine.get(stream_name, key)? {
                let mut existing_val = match &existing.value {
                    DataValue::String(s) => serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or_else(|_| serde_json::Value::String(s.clone())),
                    other => serde_json::to_value(other).unwrap_or(serde_json::Value::Null),
                };
                if let serde_json::Value::Object(ref mut existing_map) = existing_val {
                    if let serde_json::Value::Object(update_map) = value {
                        for (k, v) in update_map {
                            existing_map.insert(k.clone(), v.clone());
                        }
                    } else {
                        existing_val = value.clone();
                    }
                } else {
                    existing_val = value.clone();
                }
                let _ = engine.append(stream_name, key, json_to_datavalue(existing_val), false)?;
                affected_rows = 1;
            }
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(vec![Record {
                sequence_id: 1,
                timestamp,
                type_tag: 5,
                flags: 1,
                stream_name: stream_name.clone(),
                key: crate::storage::key::StreamKey::from_str_truncated("status"),
                value: DataValue::String(format!(
                    r#"{{"status": "success", "affected_rows": {}}}"#,
                    affected_rows
                )),
            }])
        }
        Query::DeleteKey { stream_name, key } => {
            if !engine.list_streams().contains(stream_name) {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }
            let compound_key = format!("{}:{}", stream_name, key);
            let mut affected_rows = 0;
            if engine.skipmap.contains_key(&compound_key) {
                let _ = engine.append_tombstone_batch(stream_name, &[key.clone()])?;
                affected_rows = 1;
            }
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(vec![Record {
                sequence_id: 1,
                timestamp,
                type_tag: 5,
                flags: 1,
                stream_name: stream_name.clone(),
                key: crate::storage::key::StreamKey::from_str_truncated("status"),
                value: DataValue::String(format!(
                    r#"{{"status": "success", "affected_rows": {}}}"#,
                    affected_rows
                )),
            }])
        }
        Query::Empty { stream_name } => {
            if !engine.list_streams().contains(stream_name) {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }
            let keys = engine.list_keys(stream_name);
            let affected_rows = keys.len();
            if !keys.is_empty() {
                let _ = engine.append_tombstone_batch(stream_name, &keys)?;
            }
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(vec![Record {
                sequence_id: 1,
                timestamp,
                type_tag: 5,
                flags: 1,
                stream_name: stream_name.clone(),
                key: crate::storage::key::StreamKey::from_str_truncated("status"),
                value: DataValue::String(format!(
                    r#"{{"status": "success", "affected_rows": {}}}"#,
                    affected_rows
                )),
            }])
        }
        Query::Drop { stream_name } => {
            if !engine.list_streams().contains(stream_name) {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }
            let keys = engine.list_keys(stream_name);
            let affected_rows = keys.len();
            if !keys.is_empty() {
                let _ = engine.append_tombstone_batch(stream_name, &keys)?;
            }
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(vec![Record {
                sequence_id: 1,
                timestamp,
                type_tag: 5,
                flags: 1,
                stream_name: stream_name.clone(),
                key: crate::storage::key::StreamKey::from_str_truncated("status"),
                value: DataValue::String(format!(
                    r#"{{"status": "success", "affected_rows": {}}}"#,
                    affected_rows
                )),
            }])
        }
        Query::PipelineUpdate {
            pipeline,
            update_value,
        } => {
            let stream_name = if let Some(PipelineStage::From { stream_name }) = pipeline.first() {
                stream_name.clone()
            } else {
                return Err("Pipeline query must start with a from() stage".to_string());
            };
            if !engine.list_streams().contains(&stream_name) {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }
            let mut records = engine.scan_historical()?;
            apply_pipeline_stages_to_vec(&mut records, engine, pipeline);
            let affected_rows = records.len();
            for r in &records {
                let mut val = match &r.value {
                    DataValue::String(s) => serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or_else(|_| serde_json::Value::String(s.clone())),
                    other => serde_json::to_value(other).unwrap_or(serde_json::Value::Null),
                };
                if let serde_json::Value::Object(ref mut map) = val {
                    if let serde_json::Value::Object(up_map) = update_value {
                        for (k, v) in up_map {
                            map.insert(k.clone(), v.clone());
                        }
                    } else {
                        val = update_value.clone();
                    }
                } else {
                    val = update_value.clone();
                }
                let _ = engine.append(
                    &r.stream_name,
                    r.key.as_str(),
                    json_to_datavalue(val),
                    false,
                )?;
            }
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(vec![Record {
                sequence_id: 1,
                timestamp,
                type_tag: 5,
                flags: 1,
                stream_name,
                key: crate::storage::key::StreamKey::from_str_truncated("status"),
                value: DataValue::String(format!(
                    r#"{{"status": "success", "affected_rows": {}}}"#,
                    affected_rows
                )),
            }])
        }
        Query::PipelineDelete { pipeline } => {
            let stream_name = if let Some(PipelineStage::From { stream_name }) = pipeline.first() {
                stream_name.clone()
            } else {
                return Err("Pipeline query must start with a from() stage".to_string());
            };
            if !engine.list_streams().contains(&stream_name) {
                return Err(format!("Stream '{}' does not exist", stream_name));
            }

            let mut affected_rows = 0;

            // Check if there is an index-assisted unique key constraint
            if let Some(key) = extract_unique_key_from_pipeline(pipeline) {
                if let Some(record) = engine.get(&stream_name, &key)? {
                    let mut single_rec_vec = vec![record];
                    apply_pipeline_stages_to_vec(&mut single_rec_vec, engine, pipeline);
                    if !single_rec_vec.is_empty() {
                        let _ = engine.append_tombstone_batch(&stream_name, &[key])?;
                        affected_rows = 1;
                    }
                }
            } else {
                let mut records = engine.scan_historical()?;
                apply_pipeline_stages_to_vec(&mut records, engine, pipeline);

                let mut unique_keys = std::collections::HashSet::new();
                let mut target_keys = Vec::new();
                for r in &records {
                    let k_str = r.key.to_string();
                    if unique_keys.insert(k_str.clone()) {
                        target_keys.push(k_str);
                    }
                }

                if !target_keys.is_empty() {
                    let _ = engine.append_tombstone_batch(&stream_name, &target_keys)?;
                }
                affected_rows = target_keys.len();
            }

            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(vec![Record {
                sequence_id: 1,
                timestamp,
                type_tag: 5,
                flags: 1,
                stream_name,
                key: crate::storage::key::StreamKey::from_str_truncated("status"),
                value: DataValue::String(format!(
                    r#"{{"status": "success", "affected_rows": {}}}"#,
                    affected_rows
                )),
            }])
        }
        Query::ListStreams => {
            let streams = engine.list_streams();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let records = streams
                .into_iter()
                .map(|stream| Record {
                    sequence_id: 0,
                    timestamp,
                    type_tag: 5,
                    flags: 0,
                    stream_name: "streams".to_string(),
                    key: crate::storage::key::StreamKey::from_str_truncated(&stream),
                    value: DataValue::String(stream),
                })
                .collect();
            Ok(records)
        }
        Query::Status => {
            let max_connections = engine.max_connections;
            let active_connections = max_connections - engine.conn_semaphore.available_permits();
            let broadcast_capacity = engine.broadcast_capacity;

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let value_str = format!(
                r#"{{"max_connections":{},"active_connections":{},"broadcast_capacity":{}}}"#,
                max_connections, active_connections, broadcast_capacity
            );

            Ok(vec![Record {
                sequence_id: 0,
                timestamp,
                type_tag: 5,
                flags: 0,
                stream_name: "status".to_string(),
                key: crate::storage::key::StreamKey::from_str_truncated("metrics"),
                value: DataValue::String(value_str),
            }])
        }
    }
}

/// Executes a pipeline query on static historical database records.
pub fn execute_query_historical(engine: &StorageEngine, stages: &[PipelineStage]) -> Vec<Record> {
    let mut records = engine.scan_historical().unwrap_or_default();
    apply_pipeline_stages_to_vec(&mut records, engine, stages);
    records
}

/// Helper to apply stages sequentially on a Vector.
pub fn apply_pipeline_stages_to_vec(
    records: &mut Vec<Record>,
    engine: &StorageEngine,
    stages: &[PipelineStage],
) {
    for stage in stages {
        match stage {
            PipelineStage::From { stream_name } => {
                records.retain(|r| r.stream_name == *stream_name);
            }
            PipelineStage::Delete => {
                records.retain(|r| {
                    (r.flags & 0x02 != 0 || r.value == DataValue::Null) && r.flags & 0x04 == 0
                });
            }
            PipelineStage::Trash => {
                records.retain(|r| r.flags & 0x04 != 0 || r.key.as_str() == "*");
            }
            PipelineStage::Filter { expr } => {
                records.retain(|r| evaluate_filter_expr(r, expr));
            }
            PipelineStage::VectorFilter {
                field,
                query_vector,
                threshold,
            } => {
                records.retain(|r| {
                    if let Some(val) = evaluate_record_field(r, field)
                        && let Some(record_vec) = to_vector(&val)
                        && let Some(sim) = cosine_similarity(&record_vec, query_vector)
                    {
                        return sim >= threshold.into_inner();
                    }
                    false
                });
            }
            PipelineStage::Get { key } => {
                records.retain(|r| r.key.as_str() == *key);
            }
            PipelineStage::Count => {
                let count = records.len() as u64;
                let stream_name = records
                    .first()
                    .map(|r| r.stream_name.clone())
                    .unwrap_or_else(|| "stream".to_string());
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                *records = vec![Record {
                    sequence_id: 1,
                    timestamp,
                    type_tag: 3, // UInt
                    flags: 0x01,
                    stream_name,
                    key: crate::storage::key::StreamKey::from_str_truncated("count"),
                    value: DataValue::UInt(count),
                }];
            }
            PipelineStage::Sort { field, descending } => {
                records.sort_by(|a, b| {
                    let val_a = evaluate_record_field(a, field).unwrap_or(DataValue::Null);
                    let val_b = evaluate_record_field(b, field).unwrap_or(DataValue::Null);
                    let cmp = if let (Some(na), Some(nb)) = (to_f64(&val_a), to_f64(&val_b)) {
                        ordered_float::OrderedFloat(na).cmp(&ordered_float::OrderedFloat(nb))
                    } else {
                        val_a.cmp(&val_b)
                    };
                    if *descending { cmp.reverse() } else { cmp }
                });
            }
            PipelineStage::Page {
                page_number,
                page_size,
            } => {
                if *page_number > 0 && *page_size > 0 {
                    let start = (*page_number - 1) * *page_size;
                    if start < records.len() {
                        let end = (start + *page_size).min(records.len());
                        *records = records[start..end].to_vec();
                    } else {
                        records.clear();
                    }
                }
            }
            PipelineStage::PageCursor { cursor, page_size } => {
                records.retain(|r| r.key.as_str() > cursor.as_str());
                records.truncate(*page_size);
            }
            PipelineStage::Group {
                field,
                aggregations,
            } => {
                if records.is_empty() {
                    continue;
                }
                let stream_name = records[0].stream_name.clone();
                let mut groups: BTreeMap<String, Vec<Record>> = BTreeMap::new();
                for r in records.iter() {
                    let group_key_val = evaluate_record_field(r, field).unwrap_or(DataValue::Null);
                    let group_key = match group_key_val {
                        DataValue::String(s) => s,
                        other => other.to_string(),
                    };
                    groups.entry(group_key).or_default().push(r.clone());
                }

                let mut grouped_records = Vec::new();
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                for (group_key, group_records) in groups {
                    let mut obj = serde_json::Map::new();
                    obj.insert(field.clone(), serde_json::Value::String(group_key.clone()));

                    for agg in aggregations {
                        if agg == "count()" {
                            obj.insert(
                                "count".to_string(),
                                serde_json::Value::Number((group_records.len() as u64).into()),
                            );
                        } else if agg.starts_with("sum(") && agg.ends_with(')') {
                            let arg = &agg[4..agg.len() - 1];
                            let mut sum_val = 0.0;
                            for r in &group_records {
                                if let Some(v) = evaluate_record_field(r, arg)
                                    && let Some(n) = to_f64(&v)
                                {
                                    sum_val += n;
                                }
                            }
                            obj.insert(
                                format!("sum_{}", arg),
                                serde_json::Number::from_f64(sum_val)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null),
                            );
                        } else if agg.starts_with("avg(") && agg.ends_with(')') {
                            let arg = &agg[4..agg.len() - 1];
                            let mut sum_val = 0.0;
                            let mut count = 0;
                            for r in &group_records {
                                if let Some(v) = evaluate_record_field(r, arg)
                                    && let Some(n) = to_f64(&v)
                                {
                                    sum_val += n;
                                    count += 1;
                                }
                            }
                            let avg = if count > 0 {
                                sum_val / count as f64
                            } else {
                                0.0
                            };
                            obj.insert(
                                format!("avg_{}", arg),
                                serde_json::Number::from_f64(avg)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null),
                            );
                        } else if agg.starts_with("min(") && agg.ends_with(')') {
                            let arg = &agg[4..agg.len() - 1];
                            let mut min_val = f64::MAX;
                            let mut found = false;
                            for r in &group_records {
                                if let Some(v) = evaluate_record_field(r, arg)
                                    && let Some(n) = to_f64(&v)
                                    && n < min_val
                                {
                                    min_val = n;
                                    found = true;
                                }
                            }
                            let val = if found {
                                serde_json::Number::from_f64(min_val)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            } else {
                                serde_json::Value::Null
                            };
                            obj.insert(format!("min_{}", arg), val);
                        } else if agg.starts_with("max(") && agg.ends_with(')') {
                            let arg = &agg[4..agg.len() - 1];
                            let mut max_val = f64::MIN;
                            let mut found = false;
                            for r in &group_records {
                                if let Some(v) = evaluate_record_field(r, arg)
                                    && let Some(n) = to_f64(&v)
                                    && n > max_val
                                {
                                    max_val = n;
                                    found = true;
                                }
                            }
                            let val = if found {
                                serde_json::Number::from_f64(max_val)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            } else {
                                serde_json::Value::Null
                            };
                            obj.insert(format!("max_{}", arg), val);
                        }
                    }

                    grouped_records.push(Record {
                        sequence_id: grouped_records.len() as u64 + 1,
                        timestamp,
                        type_tag: 5, // String
                        flags: 0x01,
                        stream_name: stream_name.clone(),
                        key: crate::storage::key::StreamKey::from_str_truncated(&group_key),
                        value: DataValue::String(serde_json::Value::Object(obj).to_string()),
                    });
                }

                *records = grouped_records;
            }
            PipelineStage::Map { projections } => {
                for r in records.iter_mut() {
                    project_record(r, projections);
                }
            }
            PipelineStage::Limit { count } => {
                records.truncate(*count);
            }
            PipelineStage::Enrich {
                source_stream,
                join_key,
            } => {
                for r in records.iter_mut() {
                    enrich_record(r, engine, source_stream, join_key);
                }
            }
            PipelineStage::Window {
                duration_ms,
                strategy,
            } => {
                if records.is_empty() {
                    continue;
                }

                // Group by window start boundary
                let mut windows: BTreeMap<i64, Vec<Record>> = BTreeMap::new();
                for r in records.iter() {
                    let win_start = (r.timestamp / (*duration_ms as i64)) * (*duration_ms as i64);
                    windows.entry(win_start).or_default().push(r.clone());
                }

                let mut aggregated_records = Vec::new();
                for (win_start, group) in windows {
                    let stream_name = group[0].stream_name.clone();

                    let val = match strategy {
                        AggregateStrategy::Count => DataValue::UInt(group.len() as u64),
                        AggregateStrategy::Sum => {
                            let mut sum_f = 0.0;
                            let mut is_float = false;
                            for r in &group {
                                let num_val = to_f64(&r.value).or_else(|| {
                                    // Try to extract any field inside JSON if record value itself is not a numeric scalar
                                    if let DataValue::String(s) = &r.value
                                        && let Ok(json) =
                                            serde_json::from_str::<serde_json::Value>(s)
                                        && let Some(obj) = json.as_object()
                                    {
                                        // Find first numeric field
                                        for (_, v) in obj {
                                            if let Some(f) = v.as_f64() {
                                                return Some(f);
                                            }
                                        }
                                    }
                                    None
                                });

                                if let Some(n) = num_val {
                                    sum_f += n;
                                    is_float = true;
                                }
                            }
                            if is_float {
                                DataValue::Float(ordered_float::OrderedFloat(sum_f))
                            } else {
                                DataValue::Int(0)
                            }
                        }
                        AggregateStrategy::Average => {
                            let mut sum_f = 0.0;
                            let mut count = 0;
                            for r in &group {
                                let num_val = to_f64(&r.value).or_else(|| {
                                    if let DataValue::String(s) = &r.value
                                        && let Ok(json) =
                                            serde_json::from_str::<serde_json::Value>(s)
                                        && let Some(obj) = json.as_object()
                                    {
                                        for (_, v) in obj {
                                            if let Some(f) = v.as_f64() {
                                                return Some(f);
                                            }
                                        }
                                    }
                                    None
                                });

                                if let Some(n) = num_val {
                                    sum_f += n;
                                    count += 1;
                                }
                            }
                            if count > 0 {
                                DataValue::Float(ordered_float::OrderedFloat(sum_f / count as f64))
                            } else {
                                DataValue::Float(ordered_float::OrderedFloat(0.0))
                            }
                        }
                    };

                    aggregated_records.push(Record {
                        sequence_id: win_start as u64, // Synthesize sequential index
                        timestamp: win_start,
                        type_tag: val.type_tag(),
                        flags: 0x01,
                        stream_name,
                        key: crate::storage::key::StreamKey::from_str_truncated(&format!(
                            "window_{}",
                            win_start
                        )),
                        value: val,
                    });
                }

                *records = aggregated_records;
            }
            PipelineStage::Export { .. } => {
                // Export is treated as formatting stage in the transport layer, so no op here.
            }
        }
    }
}

/// Executes a pipeline query as a continuous live-streaming channel.
pub fn execute_query_stream(
    engine: Arc<StorageEngine>,
    stages: Vec<PipelineStage>,
) -> impl Stream<Item = Record> {
    // Stream historical records first
    let historical = execute_query_historical(&engine, &stages);

    // Setup active subscription for real-time live events
    let rx = engine.subscribe();
    let live_stream = stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(rec) => return Some((rec, rx)),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue, // Keep up
                Err(_) => return None,
            }
        }
    });

    let engine_filter = engine.clone();
    let stages_filter = stages.clone();
    let engine_map = engine;
    let stages_map = stages;

    // Create continuous stream of processed entries
    stream::iter(historical).chain(
        live_stream
            .filter(move |r| {
                // Evaluate dynamic pipe stages on incoming live records
                let mut vec = vec![r.clone()];
                apply_pipeline_stages_to_vec(&mut vec, &engine_filter, &stages_filter);
                let passes = !vec.is_empty();
                async move { passes }
            })
            .map(move |r| {
                let mut vec = vec![r];
                apply_pipeline_stages_to_vec(&mut vec, &engine_map, &stages_map);
                vec[0].clone()
            }),
    )
}
