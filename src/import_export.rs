use crate::types::DataValue;
use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonlRecord {
    pub stream: String,
    pub key: String,
    pub value: serde_json::Value,

    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub type_tag: Option<u8>,
    #[serde(default)]
    pub flags: Option<u8>,

    // Ignored on import
    #[serde(default, skip_deserializing)]
    pub sequence_id: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BinaryRecord {
    pub stream: String,
    pub key: String,
    pub value: DataValue,
    pub timestamp: i64,
    pub type_tag: u8,
    pub flags: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BinaryExport {
    pub version: u32,
    pub created_at: i64,
    pub records: Vec<BinaryRecord>,
}

pub const BINARY_MAGIC: &[u8] = b"LIVEN01";

/// Convert JSON value to DataValue, handling base64 encoding for binary data
pub fn json_to_datavalue_with_base64(json: serde_json::Value) -> Result<DataValue, String> {
    match json {
        serde_json::Value::Null => Ok(DataValue::Null),
        serde_json::Value::Bool(b) => Ok(DataValue::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(DataValue::Int(i))
            } else if let Some(u) = n.as_u64() {
                Ok(DataValue::UInt(u))
            } else if let Some(f) = n.as_f64() {
                Ok(DataValue::Float(ordered_float::OrderedFloat(f)))
            } else {
                Ok(DataValue::Null)
            }
        }
        serde_json::Value::String(s) => {
            // Check if it's a base64 encoded binary value
            if s.starts_with("base64:") {
                let base64_data = &s["base64:".len()..];
                match general_purpose::STANDARD.decode(base64_data) {
                    Ok(bytes) => Ok(DataValue::Binary(bytes)),
                    Err(e) => Err(format!("Invalid base64 encoding: {}", e)),
                }
            } else {
                Ok(DataValue::String(s))
            }
        }
        serde_json::Value::Array(arr) => {
            let mut result = Vec::new();
            for item in arr {
                result.push(json_to_datavalue_with_base64(item)?);
            }
            Ok(DataValue::Array(result))
        }
        serde_json::Value::Object(obj) => {
            let mut result = BTreeMap::new();
            for (k, v) in obj {
                result.insert(k, json_to_datavalue_with_base64(v)?);
            }
            Ok(DataValue::Object(result))
        }
    }
}

/// Convert DataValue to JSON value, handling base64 encoding for binary data
pub fn datavalue_to_json(value: &DataValue) -> serde_json::Value {
    match value {
        DataValue::Null => serde_json::Value::Null,
        DataValue::Bool(b) => serde_json::Value::Bool(*b),
        DataValue::Int(i) => serde_json::Value::from(*i),
        DataValue::UInt(u) => {
            // Convert to i64 if possible, otherwise use string representation
            if let Ok(i) = i64::try_from(*u) {
                serde_json::Value::from(i)
            } else {
                serde_json::Value::String(u.to_string())
            }
        }
        DataValue::Float(f) => {
            if let Some(num) = serde_json::Number::from_f64(f.into_inner()) {
                serde_json::Value::Number(num)
            } else {
                serde_json::Value::Null
            }
        }
        DataValue::String(s) => serde_json::Value::String(s.clone()),
        DataValue::Binary(b) => {
            serde_json::Value::String(format!("base64:{}", general_purpose::STANDARD.encode(b)))
        }
        DataValue::Array(arr) => {
            let mut result = Vec::new();
            for item in arr {
                result.push(datavalue_to_json(item));
            }
            serde_json::Value::Array(result)
        }
        DataValue::Object(obj) => {
            let mut result = serde_json::Map::new();
            for (k, v) in obj {
                result.insert(k.clone(), datavalue_to_json(v));
            }
            serde_json::Value::Object(result)
        }
        DataValue::Vector(v) => {
            let mut result = Vec::new();
            for &item in v {
                result.push(serde_json::Value::from(item));
            }
            serde_json::Value::Array(result)
        }
    }
}

/// Validate a JSONL import file and collect stream information
pub fn validate_jsonl_file(path: &Path) -> Result<ValidationReport, String> {
    let file = fs::File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = io::BufReader::new(file);

    let mut streams = Vec::new();
    let mut record_count = 0;
    let mut errors = Vec::new();

    for (line_num, line_res) in reader.lines().enumerate() {
        let line: String = line_res.map_err(|e| format!("Failed to read line: {}", e))?;
        let line_num = line_num + 1; // 1-based line number

        if line.trim().is_empty() {
            continue;
        }

        let parsed: JsonlRecord = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(e) => {
                errors.push(format!("Line {}: Invalid JSON: {}", line_num, e));
                continue;
            }
        };

        // Validate required fields
        if parsed.stream.is_empty() {
            errors.push(format!(
                "Line {}: Missing required field 'stream'",
                line_num
            ));
        }

        if parsed.key.is_empty() {
            errors.push(format!("Line {}: Missing required field 'key'", line_num));
        } else if parsed.key.len() > 32 {
            errors.push(format!(
                "Line {}: Key exceeds 32 bytes: '{}'",
                line_num, parsed.key
            ));
        }

        // Validate value field
        match json_to_datavalue_with_base64(parsed.value.clone()) {
            Ok(_) => {}
            Err(e) => errors.push(format!("Line {}: Invalid value: {}", line_num, e)),
        }

        if !errors.is_empty() {
            continue;
        }

        // Collect stream information
        if !streams.contains(&parsed.stream) {
            streams.push(parsed.stream.clone());
        }

        record_count += 1;
    }

    if !errors.is_empty() {
        return Err(format!("Validation errors:\n{}", errors.join("\n")));
    }

    Ok(ValidationReport {
        record_count,
        streams,
    })
}

/// Check if streams exist and have data
pub async fn check_stream_conflicts(
    client: &mut crate::client::LivenClient,
    streams: &[String],
) -> Result<Vec<String>, String> {
    let mut conflicts = Vec::new();

    for stream in streams {
        // Check if stream exists
        let stream_exists = match client.query("streams()").await {
            Ok(records) => records.into_iter().any(|r| match r.value {
                DataValue::String(s) => s == *stream,
                _ => false,
            }),
            Err(e) => return Err(format!("Failed to check stream existence: {}", e)),
        };

        if !stream_exists {
            continue;
        }

        // Check if stream has records
        let query = format!("from(\"{}\") | limit(1)", stream);
        let has_records = match client.query(&query).await {
            Ok(records) => !records.is_empty(),
            Err(e) => {
                return Err(format!(
                    "Failed to check stream '{}' for records: {}",
                    stream, e
                ));
            }
        };

        if has_records {
            conflicts.push(stream.clone());
        }
    }

    Ok(conflicts)
}

/// Import records from JSONL file
pub async fn import_jsonl_file(
    client: &mut crate::client::LivenClient,
    path: &Path,
) -> Result<ImportStats, String> {
    let file = fs::File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = io::BufReader::new(file);

    let mut imported_count = 0;
    let skipped_count = 0;

    for (line_num, line_res) in reader.lines().enumerate() {
        let line: String = line_res.map_err(|e| format!("Failed to read line: {}", e))?;
        let line_num = line_num + 1; // 1-based line number

        if line.trim().is_empty() {
            continue;
        }

        let record: JsonlRecord = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(e) => {
                return Err(format!("Line {}: Invalid JSON: {}", line_num, e));
            }
        };

        // Convert value
        let value = match json_to_datavalue_with_base64(record.value) {
            Ok(value) => value,
            Err(e) => return Err(format!("Line {}: Invalid value: {}", line_num, e)),
        };

        // Use provided timestamp or generate current one
        let timestamp = record.timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        // Use provided type_tag or auto-detect
        let type_tag = record.type_tag.unwrap_or_else(|| value.type_tag());

        // Use provided flags or default to 0
        let flags = record.flags.unwrap_or(0);

        // Insert the record
        let query = format!(
            "from(\"{}\").insert([\"{}\", {}, {}, {}, {}])",
            record.stream,
            record.key,
            serde_json::to_string(&datavalue_to_json(&value)).unwrap(),
            timestamp,
            type_tag,
            flags
        );

        match client.query(&query).await {
            Ok(_) => imported_count += 1,
            Err(e) => {
                return Err(format!("Line {}: Failed to import record: {}", line_num, e));
            }
        }
    }

    Ok(ImportStats {
        imported_count,
        skipped_count,
    })
}

/// Export records to JSONL file
pub async fn export_jsonl_file(
    client: &mut crate::client::LivenClient,
    stream_name: &str,
    path: &Path,
) -> Result<usize, String> {
    let query = format!("from(\"{}\")", stream_name);
    let records = match client.query(&query).await {
        Ok(records) => records,
        Err(e) => return Err(format!("Failed to query stream '{}': {}", stream_name, e)),
    };

    let mut file = fs::File::create(path).map_err(|e| format!("Failed to create file: {}", e))?;

    for record in &records {
        let json_record = serde_json::json!({
            "stream": record.stream_name,
            "key": record.key.to_string(),
            "timestamp": record.timestamp,
            "type_tag": record.type_tag,
            "flags": record.flags,
            "value": datavalue_to_json(&record.value)
        });

        writeln!(file, "{}", json_record).map_err(|e| format!("Failed to write to file: {}", e))?;
    }

    Ok(records.len())
}

/// Export records to binary format
pub async fn export_binary_file(
    client: &mut crate::client::LivenClient,
    stream_name: &str,
    path: &Path,
) -> Result<usize, String> {
    let query = format!("from(\"{}\")", stream_name);
    let records = match client.query(&query).await {
        Ok(records) => records,
        Err(e) => return Err(format!("Failed to query stream '{}': {}", stream_name, e)),
    };

    let binary_records: Vec<BinaryRecord> = records
        .into_iter()
        .map(|record| BinaryRecord {
            stream: record.stream_name,
            key: record.key.to_string(),
            value: record.value,
            timestamp: record.timestamp,
            type_tag: record.type_tag,
            flags: record.flags,
        })
        .collect();

    let export = BinaryExport {
        version: 1,
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
        records: binary_records,
    };

    let mut data = Vec::new();
    data.extend_from_slice(BINARY_MAGIC);

    // Add checksum placeholder (4 bytes)
    let checksum_pos = data.len();
    data.extend_from_slice(&[0u8; 4]);

    // Serialize the export data
    let serialized = rmp_serde::to_vec(&export)
        .map_err(|e| format!("Failed to serialize binary export: {}", e))?;
    data.extend_from_slice(&serialized);

    // Calculate and write checksum
    let checksum = crc32fast::hash(&data[checksum_pos + 4..]);
    data[checksum_pos..checksum_pos + 4].copy_from_slice(&checksum.to_be_bytes());

    fs::write(path, &data).map_err(|e| format!("Failed to write binary file: {}", e))?;

    Ok(export.records.len())
}

/// Import records from binary file
pub async fn import_binary_file(
    client: &mut crate::client::LivenClient,
    path: &Path,
) -> Result<ImportStats, String> {
    let data = fs::read(path).map_err(|e| format!("Failed to read file: {}", e))?;

    // Verify magic bytes
    if data.len() < BINARY_MAGIC.len() + 4 {
        return Err("Invalid binary file: too short".to_string());
    }

    if &data[..BINARY_MAGIC.len()] != BINARY_MAGIC {
        return Err("Invalid binary file: wrong magic bytes".to_string());
    }

    // Verify checksum
    let checksum_pos = BINARY_MAGIC.len();
    let expected_checksum = u32::from_be_bytes(
        data[checksum_pos..checksum_pos + 4]
            .try_into()
            .map_err(|_| "Invalid checksum format".to_string())?,
    );
    let actual_checksum = crc32fast::hash(&data[checksum_pos + 4..]);

    if expected_checksum != actual_checksum {
        return Err("Invalid binary file: checksum mismatch".to_string());
    }

    // Deserialize the export data
    let export: BinaryExport = rmp_serde::from_slice(&data[checksum_pos + 4..])
        .map_err(|e| format!("Failed to deserialize binary export: {}", e))?;

    if export.version != 1 {
        return Err(format!(
            "Unsupported binary format version: {}",
            export.version
        ));
    }

    let mut imported_count = 0;
    let skipped_count = 0;

    for record in &export.records {
        let query = format!(
            "from(\"{}\").insert([\"{}\", {}, {}, {}, {}])",
            record.stream,
            record.key,
            serde_json::to_string(&datavalue_to_json(&record.value)).unwrap(),
            record.timestamp,
            record.type_tag,
            record.flags
        );

        match client.query(&query).await {
            Ok(_) => imported_count += 1,
            Err(e) => {
                return Err(format!(
                    "Failed to import record from '{}': {}",
                    record.stream, e
                ));
            }
        }
    }

    Ok(ImportStats {
        imported_count,
        skipped_count,
    })
}

#[derive(Debug)]
pub struct ValidationReport {
    pub record_count: usize,
    pub streams: Vec<String>,
}

#[derive(Debug)]
pub struct ImportStats {
    pub imported_count: usize,
    pub skipped_count: usize,
}
