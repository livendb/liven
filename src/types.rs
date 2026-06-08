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
        }
    }
}
