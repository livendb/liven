use crate::error::LivenError;
use crate::storage::StorageEngine;
use crate::types::Query;
use crate::types::{AggregateStrategy, DataValue, FilterExpr, Op, PipelineStage, Record};
use futures_util::stream::{self, Stream, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Validates that a key does not exceed the 32-byte limit.
fn validate_key_length(key: &str) -> crate::error::Result<()> {
    if key.len() > 32 {
        return Err(LivenError::KeyTooLong {
            len: key.len(),
            key: key.to_string(),
        });
    }
    Ok(())
}

/// Finite state machine for ordered event pattern detection within a time window.
/// Processes records sequentially and emits the record that completes each sequence.
pub struct SequenceMatcher {
    steps: Vec<FilterExpr>,
    current_step: usize,
    window_start_ms: i64,
    within_ms: u64,
    partial_matches: Vec<Record>,
}

impl SequenceMatcher {
    /// Creates a new SequenceMatcher for the given steps and time window.
    pub fn new(steps: Vec<FilterExpr>, within_ms: u64) -> Self {
        Self {
            steps,
            current_step: 0,
            window_start_ms: 0,
            within_ms,
            partial_matches: Vec::new(),
        }
    }

    /// Process one record. Returns Some(record) if this record completes the sequence.
    /// Returns None otherwise.
    pub fn process(&mut self, record: &Record) -> Option<Record> {
        if self.current_step == 0 {
            if evaluate_filter_expr(record, &self.steps[0]) {
                self.window_start_ms = record.timestamp;
                self.current_step = 1;
                self.partial_matches.push(record.clone());
                if self.current_step >= self.steps.len() {
                    self.reset();
                }
            }
            None
        } else if self.current_step > 0 {
            // Check window expiry
            if record.timestamp - self.window_start_ms > self.within_ms as i64 {
                self.reset();
                // Re-process this record from step 1
                if evaluate_filter_expr(record, &self.steps[0]) {
                    self.window_start_ms = record.timestamp;
                    self.current_step = 1;
                    self.partial_matches.push(record.clone());
                }
                return None;
            }

            if evaluate_filter_expr(record, &self.steps[self.current_step]) {
                self.current_step += 1;
                self.partial_matches.push(record.clone());
                if self.current_step >= self.steps.len() {
                    let completed = self.partial_matches.last().cloned();
                    self.reset();
                    return completed;
                }
            }
            None
        } else {
            None
        }
    }

    /// Reset FSM state. Called when window expires or sequence completes.
    pub fn reset(&mut self) {
        self.current_step = 0;
        self.window_start_ms = 0;
        self.partial_matches.clear();
    }
}

/// Merges the value of a target record into the source record value
/// as a nested JSON field named after the target stream.
///
/// Result shape:
/// {
///   "original_field": "value",
///   "target_stream_name": {
///     "target_field": "value"
///   }
/// }
pub fn merge_record_values(source: &Record, target: &Record, target_stream: &str) -> DataValue {
    let source_json = match &source.value {
        DataValue::String(s) => serde_json::from_str::<serde_json::Value>(s)
            .unwrap_or(serde_json::Value::String(s.clone())),
        other => datavalue_to_json(other),
    };

    let target_json = match &target.value {
        DataValue::String(s) => serde_json::from_str::<serde_json::Value>(s)
            .unwrap_or(serde_json::Value::String(s.clone())),
        other => datavalue_to_json(other),
    };

    let mut merged = match source_json {
        serde_json::Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("value".to_string(), other);
            map
        }
    };

    merged.insert(target_stream.to_string(), target_json);
    DataValue::String(serde_json::Value::Object(merged).to_string())
}

/// Extracts the limit count from a slice of pipeline stages, if present.
fn get_limit_from_stages(stages: &[PipelineStage]) -> Option<usize> {
    for stage in stages {
        if let PipelineStage::Limit { count } = stage {
            return Some(*count);
        }
    }
    None
}

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
        FilterExpr::Not { expr } => !evaluate_filter_expr(record, expr),
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
        DataValue::Object(obj) => {
            let mut obj_map = serde_json::Map::new();
            for (k, v) in obj {
                obj_map.insert(k.clone(), datavalue_to_json(v));
            }
            serde_json::Value::Object(obj_map)
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
        Op::Contains => {
            if let (DataValue::String(s_a), DataValue::String(s_b)) = (a, b) {
                s_a.contains(s_b.as_str())
            } else {
                false
            }
        }
        Op::EndsWith => {
            if let (DataValue::String(s_a), DataValue::String(s_b)) = (a, b) {
                s_a.ends_with(s_b.as_str())
            } else {
                false
            }
        }
        Op::Between => {
            if let DataValue::Array(bounds) = b
                && bounds.len() == 2
                && let (Some(lo), Some(hi)) = (to_f64(&bounds[0]), to_f64(&bounds[1]))
                && let Some(val) = to_f64(a)
            {
                let v = ordered_float::OrderedFloat(val);
                v >= ordered_float::OrderedFloat(lo) && v <= ordered_float::OrderedFloat(hi)
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
        FilterExpr::Not { expr } => extract_key_from_filter(expr),
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

/// Analyses a parsed Query and returns a structured execution plan as Records.
/// Does not access storage. Does not execute the query.
pub fn execute_explain(engine: &StorageEngine, query: &Query) -> Vec<Record> {
    use crate::storage::key::StreamKey;
    use std::time::SystemTime;

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let mut steps: Vec<serde_json::Value> = Vec::new();

    let total_keys = engine.skipmap.len();

    match query {
        Query::Pipeline(stages) | Query::Listen { pipeline: stages } => {
            let has_key_equality = stages
                .iter()
                .any(|s| matches!(s, PipelineStage::Get { .. }))
                || stages.iter().any(|s| {
                    if let PipelineStage::Filter { expr } = s {
                        is_key_equality_filter(expr)
                    } else {
                        false
                    }
                });

            let has_limit = stages
                .iter()
                .any(|s| matches!(s, PipelineStage::Limit { .. }));

            let stream_name = stages.iter().find_map(|s| {
                if let PipelineStage::From { stream_name } = s {
                    Some(stream_name.as_str())
                } else {
                    None
                }
            });

            let stream_keys = stream_name.map(|sn| {
                let prefix = format!("{}:", sn);
                engine
                    .skipmap
                    .iter()
                    .filter(|e| e.key().starts_with(&prefix))
                    .count()
            });

            if has_key_equality {
                steps.push(make_step(
                    0,
                    "scan",
                    "O(log n)",
                    &format!(
                        "Key equality detected — uses SkipMap point lookup. Total index keys: {}",
                        total_keys
                    ),
                ));
            } else if has_limit {
                let limit_count = stages
                    .iter()
                    .find_map(|s| {
                        if let PipelineStage::Limit { count } = s {
                            Some(*count)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                steps.push(make_step(
                    0,
                    "scan",
                    "O(n) early-exit",
                    &format!(
                        "Full segment scan with early exit at {} records. Stream '{}' has ~{} indexed keys.",
                        limit_count,
                        stream_name.unwrap_or("unknown"),
                        stream_keys.unwrap_or(total_keys)
                    ),
                ));
            } else {
                steps.push(make_step(
                    0,
                    "scan",
                    "O(n)",
                    &format!(
                        "Full segment scan across all segments. Stream '{}' has ~{} indexed keys. Total index: {}.",
                        stream_name.unwrap_or("unknown"),
                        stream_keys.unwrap_or(0),
                        total_keys
                    ),
                ));
            }

            for (step_idx, stage) in stages.iter().enumerate() {
                let (stage_name, cost, note) = describe_stage(stage, engine);
                steps.push(make_step(step_idx + 1, &stage_name, &cost, &note));
            }
        }

        Query::Insert {
            stream_name, key, ..
        } => {
            steps.push(make_step(
                0,
                "existence_check",
                "O(log n)",
                &format!(
                    "SkipMap lookup for key '{}' in stream '{}'",
                    key, stream_name
                ),
            ));
            steps.push(make_step(
                1,
                "append",
                "O(1)",
                "Enqueue to ring buffer. Single sync_data call via group-commit flusher.",
            ));
            steps.push(make_step(
                2,
                "index_update",
                "O(log n)",
                "SkipMap insert after flusher confirms write.",
            ));
        }

        Query::InsertBatch { stream_name, batch } => {
            steps.push(make_step(
                0,
                "write_lock",
                "O(1)",
                "Acquires write_lock mutex to prevent concurrent insert races.",
            ));
            steps.push(make_step(
                1,
                "existence_check",
                "O(k log n)",
                &format!(
                    "{} SkipMap lookups for stream '{}'",
                    batch.len(),
                    stream_name
                ),
            ));
            steps.push(make_step(
                2,
                "batch_append",
                "O(k)",
                &format!(
                    "{} records enqueued. Single write_all + sync_data via group-commit.",
                    batch.len()
                ),
            ));
            steps.push(make_step(
                3,
                "index_update",
                "O(k log n)",
                "SkipMap insert for each record after flusher confirms.",
            ));
        }

        Query::Upsert {
            stream_name, key, ..
        }
        | Query::Update {
            stream_name, key, ..
        } => {
            steps.push(make_step(
                0,
                "append",
                "O(1)",
                &format!(
                    "Enqueue write for key '{}' in stream '{}'. No existence check.",
                    key, stream_name
                ),
            ));
            steps.push(make_step(
                1,
                "index_update",
                "O(log n)",
                "SkipMap pointer updated to new segment offset.",
            ));
        }

        Query::UpsertBatch { stream_name, batch } => {
            steps.push(make_step(
                0,
                "batch_append",
                "O(k)",
                &format!(
                    "{} records upserted in stream '{}'. Single write_all + sync_data via group-commit.",
                    batch.len(), stream_name
                ),
            ));
            steps.push(make_step(
                1,
                "index_update",
                "O(k log n)",
                "SkipMap upsert for each record after flusher confirms.",
            ));
        }

        Query::DeleteKey { stream_name, key } => {
            steps.push(make_step(
                0,
                "existence_check",
                "O(log n)",
                &format!("SkipMap lookup for '{}:{}'", stream_name, key),
            ));
            steps.push(make_step(
                1,
                "tombstone",
                "O(1)",
                "Append tombstone frame. SkipMap entry removed.",
            ));
        }

        Query::Empty { stream_name } | Query::Drop { stream_name } => {
            let prefix = format!("{}:", stream_name);
            let key_count = engine
                .skipmap
                .iter()
                .filter(|e| e.key().starts_with(&prefix))
                .count();
            steps.push(make_step(
                0,
                "list_keys",
                "O(k)",
                &format!("{} keys found in stream '{}'", key_count, stream_name),
            ));
            steps.push(make_step(
                1,
                "tombstone_batch",
                "O(k)",
                &format!(
                    "Append {} tombstone frames in one ring buffer call. Single sync_data.",
                    key_count
                ),
            ));
        }

        Query::PipelineDelete { pipeline } | Query::PipelineUpdate { pipeline, .. } => {
            steps.push(make_step(
                0,
                "scan",
                "O(n)",
                "Full historical scan required to identify matching records.",
            ));
            steps.push(make_step(
                1,
                "pipeline_filter",
                "O(n)",
                "Apply pipeline stages in-memory to isolate targets.",
            ));
            steps.push(make_step(
                2,
                "tombstone_or_append",
                "O(k)",
                "Write tombstone or new value for each matched record.",
            ));
            let _ = pipeline;
        }

        Query::ListStreams => {
            steps.push(make_step(
                0,
                "streams_set",
                "O(s)",
                "Reads from in-memory HashSet. No disk access.",
            ));
        }

        Query::Status => {
            steps.push(make_step(
                0,
                "status",
                "O(1)",
                "Reads atomic counters. No disk access.",
            ));
        }

        Query::Explain { .. } => {
            steps.push(make_step(
                0,
                "explain",
                "O(1)",
                "Cannot explain an explain query.",
            ));
        }
    }

    let has_secondary_scan = steps.iter().any(|s| {
        s.get("note")
            .and_then(|n| n.as_str())
            .map(|n| n.contains("secondary scan"))
            .unwrap_or(false)
    });

    let warnings: Vec<&str> = steps
        .iter()
        .filter_map(|s| {
            s.get("note")
                .and_then(|n| n.as_str())
                .filter(|n| n.contains("WARNING"))
        })
        .collect();

    let plan_json = serde_json::json!({
        "total_steps": steps.len(),
        "has_secondary_scan": has_secondary_scan,
        "warnings": warnings,
        "steps": steps,
    });

    vec![Record {
        sequence_id: 0,
        timestamp,
        type_tag: 5,
        flags: 0,
        stream_name: "explain".to_string(),
        key: StreamKey::from_str_truncated("plan"),
        value: DataValue::String(plan_json.to_string()),
    }]
}

fn make_step(step: usize, stage: &str, cost: &str, note: &str) -> serde_json::Value {
    serde_json::json!({
        "step": step,
        "stage": stage,
        "cost": cost,
        "note": note,
    })
}

fn describe_stage(stage: &PipelineStage, engine: &StorageEngine) -> (String, String, String) {
    match stage {
        PipelineStage::From { stream_name } => {
            let prefix = format!("{}:", stream_name);
            let count = engine
                .skipmap
                .iter()
                .filter(|e| e.key().starts_with(&prefix))
                .count();
            (
                "from".to_string(),
                "O(n)".to_string(),
                format!(
                    "Retain records for stream '{}'. ~{} indexed keys.",
                    stream_name, count
                ),
            )
        }
        PipelineStage::Filter { .. } => (
            "filter".to_string(),
            "O(n)".to_string(),
            "Evaluate filter expression on each record in-memory.".to_string(),
        ),
        PipelineStage::Get { key } => (
            "get".to_string(),
            "O(n)".to_string(),
            format!(
                "Retain records where key == '{}'. Applied after scan — not index-assisted at this stage.",
                key
            ),
        ),
        PipelineStage::VectorFilter {
            field,
            query_vector,
            threshold,
        } => (
            "vector_filter".to_string(),
            "O(n * d)".to_string(),
            format!(
                "Cosine similarity on field '{}'. Query vector dim: {}. Threshold: {:.3}. No index — full scan comparison.",
                field,
                query_vector.len(),
                threshold.into_inner()
            ),
        ),
        PipelineStage::Correlate {
            source_stream,
            join_key,
            within_ms,
        } => (
            "correlate".to_string(),
            "O(n * m) WARNING".to_string(),
            format!(
                "WARNING: Secondary full scan triggered for stream '{}' on join key '{}' within {}ms. \
                This is an O(n*m) nested loop. Place restrictive filters before correlate to reduce n. \
                Avoid on large datasets until stream-scoped index is implemented.",
                source_stream, join_key, within_ms
            ),
        ),
        PipelineStage::Sequence { steps, within_ms } => (
            "sequence".to_string(),
            "O(n)".to_string(),
            format!(
                "FSM with {} steps over time window of {}ms. Records are sorted by timestamp before processing. \
                Only the final completing record of each sequence is emitted.",
                steps.len(),
                within_ms
            ),
        ),
        PipelineStage::Chain {
            target_stream,
            join_key,
        } => (
            "chain".to_string(),
            "O(n log m)".to_string(),
            format!(
                "Left join to stream '{}' on key '{}'. Each record does one SkipMap lookup. No secondary scan.",
                target_stream, join_key
            ),
        ),
        PipelineStage::Enrich {
            source_stream,
            join_key,
        } => (
            "enrich".to_string(),
            "O(n log m)".to_string(),
            format!(
                "Enrich from stream '{}' on key '{}'. One SkipMap point lookup per record. No secondary scan.",
                source_stream, join_key
            ),
        ),
        PipelineStage::Map { projections } => (
            "map".to_string(),
            "O(n)".to_string(),
            format!(
                "Project fields: {}. In-memory transformation.",
                projections.join(", ")
            ),
        ),
        PipelineStage::Sort { field, descending } => (
            "sort".to_string(),
            "O(n log n)".to_string(),
            format!(
                "Sort by '{}' {}. Requires all records in memory before returning results.",
                field,
                if *descending {
                    "descending"
                } else {
                    "ascending"
                }
            ),
        ),
        PipelineStage::Limit { count } => (
            "limit".to_string(),
            "O(1)".to_string(),
            format!("Truncate to {} records. Applied after prior stages.", count),
        ),
        PipelineStage::Window {
            duration_ms,
            strategy,
        } => (
            "window".to_string(),
            "O(n)".to_string(),
            format!(
                "Group records into {}ms buckets. Strategy: {:?}. One output record per window.",
                duration_ms, strategy
            ),
        ),
        PipelineStage::Group {
            field,
            aggregations,
        } => (
            "group".to_string(),
            "O(n)".to_string(),
            format!(
                "Group by '{}'. Aggregations: {}. Requires all records in memory.",
                field,
                aggregations.join(", ")
            ),
        ),
        PipelineStage::Page {
            page_number,
            page_size,
        } => (
            "page".to_string(),
            "O(n)".to_string(),
            format!(
                "Offset page {}. Skips {} records. Full scan still required to reach offset.",
                page_number,
                (page_number.saturating_sub(1)) * page_size
            ),
        ),
        PipelineStage::PageCursor { cursor, page_size } => (
            "page_cursor".to_string(),
            "O(n)".to_string(),
            format!(
                "Cursor pagination after key '{}'. Page size {}. Avoids offset scan drift.",
                cursor, page_size
            ),
        ),
        PipelineStage::Count => (
            "count".to_string(),
            "O(n)".to_string(),
            "Count records after prior stages. Full scan required — not metadata-based."
                .to_string(),
        ),
        PipelineStage::Export { format } => (
            "export".to_string(),
            "O(n)".to_string(),
            format!(
                "Serialize results to {:?} format. Applied at transport layer.",
                format
            ),
        ),
        PipelineStage::Delete => (
            "delete".to_string(),
            "O(n)".to_string(),
            "Retain only tombstoned records. Used in pipeline context.".to_string(),
        ),
        PipelineStage::Trash => (
            "trash".to_string(),
            "O(n)".to_string(),
            "Retain only trashed records.".to_string(),
        ),
        PipelineStage::Distinct { field } => (
            "distinct".to_string(),
            "O(n)".to_string(),
            format!(
                "Deduplicate by field '{}'. Uses a HashSet internally.",
                field
            ),
        ),
    }
}

fn is_key_equality_filter(expr: &FilterExpr) -> bool {
    match expr {
        FilterExpr::Simple {
            field, operator, ..
        } => field == "key" && *operator == Op::Eq,
        FilterExpr::And { left, right } => {
            is_key_equality_filter(left) || is_key_equality_filter(right)
        }
        _ => false,
    }
}

/// Check if a filter expression is a time-range comparison on the `timestamp` field.
/// Returns Some((start_ms, end_ms)) if the filter is a simple time range.
fn extract_time_range_filter(expr: &FilterExpr) -> Option<(i64, i64)> {
    match expr {
        FilterExpr::Simple {
            field,
            operator,
            value,
        } if field == "timestamp" => match operator {
            Op::Gt => to_f64(value).map(|n| (n as i64 + 1, i64::MAX)),
            Op::GtEq => to_f64(value).map(|n| (n as i64, i64::MAX)),
            Op::Lt => to_f64(value).map(|n| (i64::MIN, n as i64 - 1)),
            Op::LtEq => to_f64(value).map(|n| (i64::MIN, n as i64)),
            Op::Eq => to_f64(value).map(|n| (n as i64, n as i64)),
            Op::Between => {
                if let DataValue::Array(bounds) = value
                    && bounds.len() == 2
                    && let Some(lo) = to_f64(&bounds[0])
                    && let Some(hi) = to_f64(&bounds[1])
                {
                    Some((lo as i64, hi as i64))
                } else {
                    None
                }
            }
            _ => None,
        },
        FilterExpr::And { left, right } => {
            // Merge conjunctive time ranges
            let left_range = extract_time_range_filter(left);
            let right_range = extract_time_range_filter(right);
            match (left_range, right_range) {
                (Some((l1, r1)), Some((l2, r2))) => Some((l1.min(l2), r1.max(r2))),
                (Some(r), None) | (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        _ => None,
    }
}

/// Check if a pipeline has a leading time-range filter on the `timestamp` field.
/// Returns Some((start_ms, end_ms)) if applicable.
fn has_time_range_filter(stages: &[PipelineStage]) -> Option<(i64, i64)> {
    for stage in stages {
        if let PipelineStage::Filter { expr } = stage {
            return extract_time_range_filter(expr);
        }
    }
    None
}

/// Executes any full parsed Query on the StorageEngine, returning results or execution errors.
#[tracing::instrument(skip(engine, query), fields(query_type = ?query.variant_name()))]
pub fn execute_query(engine: &StorageEngine, query: &Query) -> crate::error::Result<Vec<Record>> {
    match query {
        Query::Pipeline(stages) => {
            if let Some(PipelineStage::From { stream_name }) = stages.first()
                && !engine.list_streams().contains(stream_name)
            {
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
            }

            // Fast path: if the pipeline resolves to a single key equality
            // (e.g. from("s").get("k") or from("s") | filter(key == "k")),
            // use the skipmap O(log N) point lookup instead of scanning all segments.
            if let Some(key) = extract_unique_key_from_pipeline(stages) {
                let stream_name =
                    if let Some(PipelineStage::From { stream_name: sn }) = stages.first() {
                        sn
                    } else {
                        return Err(LivenError::Query(
                            "Pipeline must start with from()".to_string(),
                        ));
                    };
                let mut records = match engine.get(stream_name, &key)? {
                    Some(record) => vec![record],
                    None => vec![],
                };
                // Skip the From and Get/key-filter stages since they are already satisfied.
                // Apply only the remaining stages (Limit, Map, Sort, Count, etc.)
                let remaining: Vec<&PipelineStage> = stages
                    .iter()
                    .filter(|s| {
                        !matches!(s, PipelineStage::From { .. })
                            && !matches!(s, PipelineStage::Get { .. })
                            && !matches!(s, PipelineStage::Filter { expr } if is_key_equality_filter(expr))
                    })
                    .collect();
                let remaining_refs: Vec<PipelineStage> = remaining.into_iter().cloned().collect();
                apply_pipeline_stages_to_vec(&mut records, engine, &remaining_refs);
                return Ok(records);
            }

            // Fast path: time-range filter on timestamp field.
            // Use the per-stream timestamp index for O(log N + K) range scans.
            if let Some((start_ms, end_ms)) = has_time_range_filter(stages)
                && let Some(PipelineStage::From { stream_name }) = stages.first()
            {
                let mut records = engine.scan_by_time_range(stream_name, start_ms, end_ms)?;
                apply_pipeline_stages_to_vec(&mut records, engine, stages);
                return Ok(records);
            }

            // Fall through to the full historical scan for general queries.
            let mut records = engine.scan_historical()?;
            apply_pipeline_stages_to_vec(&mut records, engine, stages);
            Ok(records)
        }
        Query::Insert {
            stream_name,
            key,
            value,
        } => {
            // Validate key length at parse layer
            validate_key_length(key)?;

            // Acquire write_lock to prevent concurrent inserts for the same key
            let _guard = engine.write_lock.lock().unwrap();

            if engine.get(stream_name, key)?.is_some() {
                return Err(LivenError::Query(format!(
                    "Key '{}' already exists in stream '{}'.\n   Use upsert to overwrite an existing key or\n   update to merge changes into an existing record.",
                    key, stream_name
                )));
            }
            let record =
                engine.append(stream_name, key, json_to_datavalue(value.clone()), false)?;
            Ok(vec![record])
        }
        Query::InsertBatch { stream_name, batch } => {
            // Validate all key lengths at parse layer
            for (key, _) in batch {
                validate_key_length(key)?;
            }

            // Acquire the write lock to atomically check all keys and queue appends.
            // This prevents other concurrent InsertBatch operations from interleaving
            // between the existence check and the ring buffer enqueue.
            let _guard = engine.write_lock.lock().unwrap();

            for (key, _) in batch {
                if engine.get(stream_name, key)?.is_some() {
                    return Err(LivenError::Query(format!(
                        "Key '{}' already exists in stream '{}' (batch insertion aborted)",
                        key, stream_name
                    )));
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
            // Validate key length at parse layer
            validate_key_length(key)?;

            // Acquire write_lock to prevent concurrent operations on the same key
            let _guard = engine.write_lock.lock().unwrap();

            // Check if key exists and tombstone it if it does
            if engine.get(stream_name, key)?.is_some() {
                let _ = engine.append(stream_name, key, DataValue::Null, true)?;
            }

            let record =
                engine.append(stream_name, key, json_to_datavalue(value.clone()), false)?;
            Ok(vec![record])
        }
        Query::UpsertBatch { stream_name, batch } => {
            // Validate all key lengths at parse layer
            for (key, _) in batch {
                validate_key_length(key)?;
            }

            // Acquire write_lock to prevent concurrent operations on any keys in the batch
            let _guard = engine.write_lock.lock().unwrap();

            let mut inserted = Vec::new();
            for (key, val) in batch {
                // Check if key exists and tombstone it if it does
                if engine.get(stream_name, key)?.is_some() {
                    let _ = engine.append(stream_name, key, DataValue::Null, true)?;
                }

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
            // Validate key length at parse layer
            validate_key_length(key)?;

            if !engine.list_streams().contains(stream_name) {
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
            }

            // Acquire write_lock to prevent concurrent operations on the same key
            let _guard = engine.write_lock.lock().unwrap();

            let mut affected_rows = 0;
            if let Some(existing) = engine.get(stream_name, key)? {
                // Tombstone the old record first
                let _ = engine.append(stream_name, key, DataValue::Null, true)?;

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
            // Validate key length at parse layer
            validate_key_length(key)?;

            if !engine.list_streams().contains(stream_name) {
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
            }
            let compound_key = format!("{}:{}", stream_name, key);
            let mut affected_rows = 0;

            // Hold write_lock to prevent concurrent Insert/Upsert from racing with this delete.
            // The same lock is used by InsertBatch. Individual Insert calls do not use it
            // (last-writer-wins semantics are acceptable for single-record inserts).
            let _guard = engine.write_lock.lock().unwrap();

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
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
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
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
            }
            let keys = engine.list_keys(stream_name);
            let affected_rows = keys.len();
            if !keys.is_empty() {
                let _ = engine.append_tombstone_batch(stream_name, &keys)?;
            }
            // Remove the stream from streams_set so it no longer appears in listings
            // and no longer counts against max_concurrent_streams.
            {
                let mut streams_guard = engine.streams_set.lock().unwrap();
                streams_guard.remove(stream_name);
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
                return Err(LivenError::Query(
                    "Pipeline query must start with a from() stage".to_string(),
                ));
            };
            if !engine.list_streams().contains(&stream_name) {
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
            }
            // Acquire write_lock to prevent concurrent inserts from racing between
            // the historical scan and the append of updated records.
            let _guard = engine.write_lock.lock().unwrap();
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
                return Err(LivenError::Query(
                    "Pipeline query must start with a from() stage".to_string(),
                ));
            };
            if !engine.list_streams().contains(&stream_name) {
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
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
                // Acquire write_lock to prevent concurrent inserts from racing between
                // the historical scan and the tombstone batch.
                let _guard = engine.write_lock.lock().unwrap();
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
            let active_connections =
                engine.max_connections - engine.conn_semaphore.available_permits();
            let broadcast_subscribers = engine.broadcast_subscriber_count();

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let value_str = format!(
                r#"{{"max_connections":{},"broadcast_capacity":{},"active_connections":{},"broadcast_subscribers":{}}}"#,
                engine.max_connections,
                engine.broadcast_capacity,
                active_connections,
                broadcast_subscribers
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
        Query::Explain { inner_query } => Ok(execute_explain(engine, inner_query)),
        Query::Listen { pipeline } => {
            if let Some(PipelineStage::From { stream_name }) = pipeline.first()
                && !engine.list_streams().contains(stream_name)
            {
                return Err(LivenError::Query(format!(
                    "Stream '{}' does not exist",
                    stream_name
                )));
            }
            let mut records = if let Some(limit) = get_limit_from_stages(pipeline) {
                engine.scan_historical_limited(limit)?
            } else {
                engine.scan_historical()?
            };
            apply_pipeline_stages_to_vec(&mut records, engine, pipeline);
            Ok(records)
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
                // With Fix 1, scan_historical no longer returns tombstone frames, so this stage
                // primarily serves as a terminal parser target for PipelineDelete execution.
                // The executor handles deletion separately via append_tombstone_batch.
                records.retain(|r| r.flags & 0x02 != 0 && r.flags & 0x04 == 0);
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
            PipelineStage::Correlate {
                source_stream,
                join_key,
                within_ms,
            } => {
                // Pre-fetch all records from source_stream into a HashMap keyed by the join field value.
                // This replaces the O(N × M) double scan with O(N + M) construction + O(K) lookups.
                let all_source: Vec<Record> = engine
                    .scan_historical()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|r| r.stream_name == *source_stream)
                    .collect();

                // Build a multi-map: join_key_value → Vec<(timestamp, Record)>
                let mut source_map: std::collections::HashMap<String, Vec<(i64, Record)>> =
                    std::collections::HashMap::new();
                for other in all_source {
                    if let Some(kv) = evaluate_record_field(&other, join_key) {
                        let k = match &kv {
                            DataValue::String(s) => s.clone(),
                            DataValue::Int(i) => i.to_string(),
                            DataValue::UInt(u) => u.to_string(),
                            _ => format!("{:?}", kv),
                        };
                        source_map
                            .entry(k)
                            .or_default()
                            .push((other.timestamp, other));
                    }
                }

                let mut results = Vec::new();
                for record in records.iter() {
                    if let Some(key_val) = evaluate_record_field(record, join_key) {
                        let key_str = match &key_val {
                            DataValue::String(s) => s.clone(),
                            DataValue::Int(i) => i.to_string(),
                            DataValue::UInt(u) => u.to_string(),
                            _ => format!("{:?}", key_val),
                        };
                        let window_min = record.timestamp - *within_ms as i64;
                        let window_max = record.timestamp + *within_ms as i64;

                        let mut matched_count = 0u64;
                        let mut merged_value = datavalue_to_json(&record.value);

                        if let Some(candidates) = source_map.get(&key_str) {
                            for (ts, other) in candidates {
                                if *ts < window_min || *ts > window_max {
                                    continue;
                                }
                                matched_count += 1;
                                // Merge matched record fields into record value
                                if let serde_json::Value::Object(ref mut map) = merged_value {
                                    let other_json = match &other.value {
                                        DataValue::String(s) => {
                                            serde_json::from_str::<serde_json::Value>(s)
                                                .unwrap_or(serde_json::Value::String(s.clone()))
                                        }
                                        other_val => datavalue_to_json(other_val),
                                    };
                                    map.insert(format!("correlated_{}", matched_count), other_json);
                                }
                            }
                        }

                        if matched_count > 0 {
                            if let serde_json::Value::Object(ref mut map) = merged_value {
                                map.insert(
                                    "correlated_count".to_string(),
                                    serde_json::Value::Number(matched_count.into()),
                                );
                            }
                            let mut enriched = record.clone();
                            enriched.value = DataValue::String(merged_value.to_string());
                            results.push(enriched);
                        }
                    }
                }
                *records = results;
            }
            PipelineStage::Sequence { steps, within_ms } => {
                let mut matcher = SequenceMatcher::new(steps.clone(), *within_ms);
                let mut results = Vec::new();
                // Records must be in timestamp order for correct FSM behavior
                records.sort_by_key(|r| r.timestamp);
                for record in records.iter() {
                    if let Some(completed) = matcher.process(record) {
                        results.push(completed);
                    }
                }
                *records = results;
            }
            PipelineStage::Chain {
                target_stream,
                join_key,
            } => {
                for record in records.iter_mut() {
                    if let Some(key_val) = evaluate_record_field(record, join_key) {
                        let key_str = match &key_val {
                            DataValue::String(s) => s.clone(),
                            DataValue::Int(i) => i.to_string(),
                            DataValue::UInt(u) => u.to_string(),
                            _ => format!("{:?}", key_val),
                        };
                        if let Ok(Some(target_record)) = engine.get(target_stream, &key_str) {
                            // Merge target fields into current record value
                            record.value =
                                merge_record_values(record, &target_record, target_stream);
                        }
                        // Chain is a left join: no match retains the record unchanged
                    } else {
                        // Join key not found at top level — search inside nested merge blocks
                        // (data added by previous chain hops, e.g. "responses" -> {response_id: ...})
                        if let DataValue::String(s) = &record.value
                            && let Ok(json) = serde_json::from_str::<serde_json::Value>(s)
                            && let Some(obj) = json.as_object()
                        {
                            'nested: for (_key, val) in obj {
                                if let Some(nested) = val.as_object()
                                    && let Some(field_val) = nested.get(join_key)
                                {
                                    let key_str = match field_val {
                                        serde_json::Value::String(s) => s.clone(),
                                        other => other.to_string(),
                                    };
                                    if let Ok(Some(target_record)) =
                                        engine.get(target_stream, &key_str)
                                    {
                                        record.value = merge_record_values(
                                            record,
                                            &target_record,
                                            target_stream,
                                        );
                                    }
                                    break 'nested;
                                }
                            }
                        }
                    }
                }
            }
            PipelineStage::Distinct { field } => {
                let mut seen = std::collections::HashSet::new();
                records.retain(|r| {
                    let key_val = evaluate_record_field(r, field).unwrap_or(DataValue::Null);
                    let dedup_key = match &key_val {
                        DataValue::String(s) => s.clone(),
                        DataValue::Int(i) => i.to_string(),
                        DataValue::UInt(u) => u.to_string(),
                        DataValue::Float(f) => f.to_string(),
                        DataValue::Bool(b) => b.to_string(),
                        DataValue::Null => String::new(),
                        other => format!("{:?}", other),
                    };
                    seen.insert(dedup_key)
                });
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
