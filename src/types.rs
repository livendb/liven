use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

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
    Vector(Vec<i8>), // Native quantized vector variant
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
            DataValue::Vector(v) => write!(f, "{:?}", v),
        }
    }
}
