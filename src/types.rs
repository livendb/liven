use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(OrderedFloat<f64>),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<DataValue>),
    Object(BTreeMap<String, DataValue>), // Native structured object variant
    Vector(Vec<i8>),                     // Native quantized vector variant
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PayloadType {
    RawBytes = 0x01,
    Structured = 0x02,
    QuantizedVector = 0x03,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FilterExpr {
    Simple {
        field: String,
        operator: Op,
        value: DataValue,
    },
    And {
        left: Box<FilterExpr>,
        right: Box<FilterExpr>,
    },
    Or {
        left: Box<FilterExpr>,
        right: Box<FilterExpr>,
    },
    Not {
        expr: Box<FilterExpr>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PipelineStage {
    From {
        stream_name: String,
    },
    Filter {
        expr: FilterExpr,
    },
    VectorFilter {
        field: String,
        query_vector: Vec<i8>,
        threshold: OrderedFloat<f64>,
    },
    Get {
        key: String,
    },
    Map {
        projections: Vec<String>,
    },
    Window {
        duration_ms: u64,
        strategy: AggregateStrategy,
    },
    Limit {
        count: usize,
    },
    Export {
        format: ExportFormat,
    },
    Enrich {
        source_stream: String,
        join_key: String,
    },
    Delete,
    Trash,
    Count,
    Sort {
        field: String,
        descending: bool,
    },
    Page {
        page_number: usize,
        page_size: usize,
    },
    PageCursor {
        cursor: String,
        page_size: usize,
    },
    Group {
        field: String,
        aggregations: Vec<String>,
    },
    /// Windowed join: links records from two streams on a shared key
    /// within a time boundary. Used for behavioral correlation across
    /// streams — fraud detection, anomaly signals, session linking.
    Correlate {
        source_stream: String,
        join_key: String,
        within_ms: u64,
    },
    /// Ordered event pattern detection within a time window using a
    /// finite state machine. Used for predictive failure detection,
    /// fraud pattern matching, and behavioral flow analysis.
    Sequence {
        steps: Vec<FilterExpr>,
        within_ms: u64,
    },
    /// Causal chain traversal: follows key relationships across
    /// multiple streams hop by hop by remapping join keys at each
    /// stage. Used for AI memory linking, transaction lineage,
    /// and multi-step event tracing. Left join — no match retains
    /// the record with its original value.
    Chain {
        target_stream: String,
        join_key: String,
    },
    /// Deduplicate records by a specific field value.
    /// Retains only the first record for each unique value of the given field.
    Distinct {
        field: String,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Op {
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    In,
    StartsWith,
    Contains, // substring match on DataValue::String
    EndsWith, // suffix match on DataValue::String
    Between,  // inclusive range check, expects DataValue::Array([low, high])
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggregateStrategy {
    Count,
    Sum,
    Average,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Csv,
    MsgPack,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Query {
    Pipeline(Vec<PipelineStage>),
    Insert {
        stream_name: String,
        key: String,
        value: serde_json::Value,
    },
    InsertBatch {
        stream_name: String,
        batch: Vec<(String, serde_json::Value)>,
    },
    Upsert {
        stream_name: String,
        key: String,
        value: serde_json::Value,
    },
    UpsertBatch {
        stream_name: String,
        batch: Vec<(String, serde_json::Value)>,
    },
    Update {
        stream_name: String,
        key: String,
        value: serde_json::Value,
    },
    DeleteKey {
        stream_name: String,
        key: String,
    },
    Empty {
        stream_name: String,
    },
    Drop {
        stream_name: String,
    },
    PipelineUpdate {
        pipeline: Vec<PipelineStage>,
        update_value: serde_json::Value,
    },
    PipelineDelete {
        pipeline: Vec<PipelineStage>,
    },
    ListStreams,
    Status,
    /// Real-time subscription query: executes the pipeline historically
    /// then streams matching live records. Used via .listen() suffix.
    Listen {
        pipeline: Vec<PipelineStage>,
    },
    /// Explain the execution plan of a query without running it.
    /// Returns a Vec<Record> describing each pipeline stage and its estimated cost.
    Explain {
        inner_query: Box<Query>,
    },
}

impl Query {
    /// Returns a static string describing the variant for use in tracing spans.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Query::Pipeline(_) => "pipeline",
            Query::Insert { .. } => "insert",
            Query::InsertBatch { .. } => "insert_batch",
            Query::Upsert { .. } => "upsert",
            Query::UpsertBatch { .. } => "upsert_batch",
            Query::Update { .. } => "update",
            Query::DeleteKey { .. } => "delete_key",
            Query::Empty { .. } => "empty",
            Query::Drop { .. } => "drop",
            Query::PipelineUpdate { .. } => "pipeline_update",
            Query::PipelineDelete { .. } => "pipeline_delete",
            Query::ListStreams => "list_streams",
            Query::Status => "status",
            Query::Listen { .. } => "listen",
            Query::Explain { .. } => "explain",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogPointer {
    pub segment_id: u64,
    pub file_offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub sequence_id: u64,
    pub timestamp: i64,
    pub type_tag: u8,
    pub flags: u8,
    pub stream_name: String,
    pub key: crate::storage::key::StreamKey,
    pub value: DataValue,
}

impl DataValue {
    pub fn type_tag(&self) -> u8 {
        match self {
            DataValue::Null => 0,
            DataValue::Bool(_) => 1,
            DataValue::Int(_) => 2,
            DataValue::UInt(_) => 3,
            DataValue::Float(_) => 4,
            DataValue::String(_) => 5,
            DataValue::Binary(_) => 6,
            DataValue::Array(_) => 7,
            DataValue::Object(_) => 9, // Skip 8 for Vector
            DataValue::Vector(_) => 8,
        }
    }
}

impl std::fmt::Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => write!(f, "null"),
            DataValue::Bool(b) => write!(f, "{}", b),
            DataValue::Int(i) => write!(f, "{}", i),
            DataValue::UInt(u) => write!(f, "{}", u),
            DataValue::Float(fl) => write!(f, "{}", fl),
            DataValue::String(s) => write!(f, "{}", s),
            DataValue::Binary(b) => write!(f, "{:?}", b),
            DataValue::Array(arr) => {
                write!(f, "[")?;
                for (i, val) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            DataValue::Object(obj) => {
                write!(f, "{{")?;
                for (i, (key, val)) in obj.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, val)?;
                }
                write!(f, "}}")
            }
            DataValue::Vector(v) => write!(f, "{:?}", v),
        }
    }
}

/// Convert serde_json::Value to DataValue, recursively handling nested structures
pub fn json_to_datavalue(json: serde_json::Value) -> DataValue {
    match json {
        serde_json::Value::Null => DataValue::Null,
        serde_json::Value::Bool(b) => DataValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DataValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                DataValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                DataValue::Float(OrderedFloat(f))
            } else {
                DataValue::Null
            }
        }
        serde_json::Value::String(s) => DataValue::String(s),
        serde_json::Value::Array(arr) => {
            DataValue::Array(arr.into_iter().map(json_to_datavalue).collect())
        }
        serde_json::Value::Object(obj) => DataValue::Object(
            obj.into_iter()
                .map(|(k, v)| (k, json_to_datavalue(v)))
                .collect(),
        ),
    }
}

/// Parse a JSON string into DataValue, handling both raw strings and nested JSON
pub fn parse_json_to_datavalue(s: &str) -> DataValue {
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(s) {
        json_to_datavalue(json_value)
    } else {
        DataValue::String(s.to_string())
    }
}
