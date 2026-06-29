//! Typed query builders for the LIVEN database engine.
//!
//! This module provides a fluent Rust API for constructing queries
//! without writing raw query strings. Use the builders to compose
//! pipelines, filters, and mutations with full type safety.
//!
//! # Examples
//!
//! ```rust
//! use liven::query::{Pipeline, Filter};
//!
//! // Build a pipeline with typed filters
//! let pipeline = Pipeline::from("events")
//!     .filter(Filter::field("type").eq("click"))
//!     .filter(Filter::field("value").gt(100.0))
//!     .limit(50);
//!
//! // Convert to a Query for execution
//! let query: liven::types::Query = pipeline.build();
//! ```

use crate::types::{AggregateStrategy, DataValue, FilterExpr, Op, PipelineStage, Query};
use ordered_float::OrderedFloat;
use serde_json::Value as JsonValue;

// ─── Query constructors ───────────────────────────────────────────────

impl Query {
    /// Create an INSERT query.
    pub fn insert(
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: JsonValue,
    ) -> Self {
        Query::Insert {
            stream_name: stream_name.into(),
            key: key.into(),
            value,
        }
    }

    /// Create a batch INSERT query.
    pub fn insert_batch(stream_name: impl Into<String>, batch: Vec<(String, JsonValue)>) -> Self {
        Query::InsertBatch {
            stream_name: stream_name.into(),
            batch,
        }
    }

    /// Create an UPSERT query.
    pub fn upsert(
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: JsonValue,
    ) -> Self {
        Query::Upsert {
            stream_name: stream_name.into(),
            key: key.into(),
            value,
        }
    }

    /// Create a batch UPSERT query.
    pub fn upsert_batch(stream_name: impl Into<String>, batch: Vec<(String, JsonValue)>) -> Self {
        Query::UpsertBatch {
            stream_name: stream_name.into(),
            batch,
        }
    }

    /// Create an UPDATE query (merges value with existing record).
    pub fn update(
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: JsonValue,
    ) -> Self {
        Query::Update {
            stream_name: stream_name.into(),
            key: key.into(),
            value,
        }
    }

    /// Create a DELETE query for a specific key.
    pub fn delete(stream_name: impl Into<String>, key: impl Into<String>) -> Self {
        Query::DeleteKey {
            stream_name: stream_name.into(),
            key: key.into(),
        }
    }

    /// Create an EMPTY query (clears all records from a stream).
    pub fn empty(stream_name: impl Into<String>) -> Self {
        Query::Empty {
            stream_name: stream_name.into(),
        }
    }

    /// Create a DROP query (removes a stream entirely).
    pub fn drop_stream(stream_name: impl Into<String>) -> Self {
        Query::Drop {
            stream_name: stream_name.into(),
        }
    }

    /// Create a LIST STREAMS query.
    pub fn list_streams() -> Self {
        Query::ListStreams
    }

    /// Create a STATUS query.
    pub fn status() -> Self {
        Query::Status
    }
}

// ─── Pipeline builder ─────────────────────────────────────────────────

/// A typed builder for constructing pipeline queries.
///
/// Pipelines chain stages — from, filter, map, limit, sort, etc. —
/// and are the primary way to read, transform, and aggregate data.
///
/// # Example
///
/// ```rust
/// use liven::query::{Pipeline, Filter};
///
/// let pipeline = Pipeline::from("orders")
///     .filter(Filter::field("amount").gte(100.0))
///     .sort("amount", true)
///     .limit(10);
/// ```
#[derive(Clone, Debug)]
pub struct Pipeline {
    stages: Vec<PipelineStage>,
}

impl Pipeline {
    /// Start a pipeline from the given stream.
    pub fn from(stream_name: impl Into<String>) -> Self {
        Self {
            stages: vec![PipelineStage::From {
                stream_name: stream_name.into(),
            }],
        }
    }

    /// Add a filter stage.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.stages.push(PipelineStage::Filter { expr: filter.0 });
        self
    }

    /// Add a vector similarity filter stage.
    pub fn vector_filter(
        mut self,
        field: impl Into<String>,
        query_vector: Vec<i8>,
        threshold: f64,
    ) -> Self {
        self.stages.push(PipelineStage::VectorFilter {
            field: field.into(),
            query_vector,
            threshold: OrderedFloat(threshold),
        });
        self
    }

    /// Add a get-by-key stage.
    pub fn get(mut self, key: impl Into<String>) -> Self {
        self.stages.push(PipelineStage::Get { key: key.into() });
        self
    }

    /// Add a map (field projection) stage.
    pub fn map(mut self, fields: Vec<String>) -> Self {
        self.stages.push(PipelineStage::Map {
            projections: fields,
        });
        self
    }

    /// Add a time-windowed aggregation stage.
    pub fn window(mut self, duration_ms: u64, strategy: AggregateStrategy) -> Self {
        self.stages.push(PipelineStage::Window {
            duration_ms,
            strategy,
        });
        self
    }

    /// Add a limit stage.
    pub fn limit(mut self, count: usize) -> Self {
        self.stages.push(PipelineStage::Limit { count });
        self
    }

    /// Add a sort stage.
    pub fn sort(mut self, field: impl Into<String>, descending: bool) -> Self {
        self.stages.push(PipelineStage::Sort {
            field: field.into(),
            descending,
        });
        self
    }

    /// Add a pagination stage.
    pub fn page(mut self, page_number: usize, page_size: usize) -> Self {
        self.stages.push(PipelineStage::Page {
            page_number,
            page_size,
        });
        self
    }

    /// Add a cursor-based pagination stage.
    pub fn page_cursor(mut self, cursor: impl Into<String>, page_size: usize) -> Self {
        self.stages.push(PipelineStage::PageCursor {
            cursor: cursor.into(),
            page_size,
        });
        self
    }

    /// Add a count stage.
    pub fn count(mut self) -> Self {
        self.stages.push(PipelineStage::Count);
        self
    }

    /// Add a group-by stage with aggregations.
    pub fn group(mut self, field: impl Into<String>, aggregations: Vec<String>) -> Self {
        self.stages.push(PipelineStage::Group {
            field: field.into(),
            aggregations,
        });
        self
    }

    /// Add a correlate (windowed join) stage.
    pub fn correlate(
        mut self,
        source_stream: impl Into<String>,
        join_key: impl Into<String>,
        within_ms: u64,
    ) -> Self {
        self.stages.push(PipelineStage::Correlate {
            source_stream: source_stream.into(),
            join_key: join_key.into(),
            within_ms,
        });
        self
    }

    /// Add a sequence (event pattern FSM) stage.
    pub fn sequence(mut self, steps: Vec<Filter>, within_ms: u64) -> Self {
        self.stages.push(PipelineStage::Sequence {
            steps: steps.into_iter().map(|f| f.0).collect(),
            within_ms,
        });
        self
    }

    /// Add a chain (multi-hop join) stage.
    pub fn chain(mut self, target_stream: impl Into<String>, join_key: impl Into<String>) -> Self {
        self.stages.push(PipelineStage::Chain {
            target_stream: target_stream.into(),
            join_key: join_key.into(),
        });
        self
    }

    /// Add a distinct stage (dedup by field).
    pub fn distinct(mut self, field: impl Into<String>) -> Self {
        self.stages.push(PipelineStage::Distinct {
            field: field.into(),
        });
        self
    }

    /// Add an enrich (left join) stage.
    pub fn enrich(mut self, source_stream: impl Into<String>, join_key: impl Into<String>) -> Self {
        self.stages.push(PipelineStage::Enrich {
            source_stream: source_stream.into(),
            join_key: join_key.into(),
        });
        self
    }

    /// Consume the builder and produce a `Query::Pipeline`.
    pub fn build(self) -> Query {
        Query::Pipeline(self.stages)
    }

    /// Consume the builder and produce a `Query::Listen` for live subscriptions.
    pub fn build_listen(self) -> Query {
        Query::Listen {
            pipeline: self.stages,
        }
    }

    /// Consume the builder and produce a `Query::PipelineUpdate`.
    pub fn build_update(self, update_value: JsonValue) -> Query {
        Query::PipelineUpdate {
            pipeline: self.stages,
            update_value,
        }
    }

    /// Consume the builder and produce a `Query::PipelineDelete`.
    pub fn build_delete(self) -> Query {
        Query::PipelineDelete {
            pipeline: self.stages,
        }
    }
}

// ─── Filter builder ───────────────────────────────────────────────────

/// A typed filter expression builder.
///
/// # Example
///
/// ```rust
/// use liven::query::Filter;
///
/// // Simple comparisons
/// let _f1 = Filter::field("status").eq("active");
/// let _f2 = Filter::field("amount").gte(100.0);
/// let _f3 = Filter::field("name").contains("alice");
///
/// // Compound filters
/// let _f4 = Filter::and(vec![
///     Filter::field("status").eq("active"),
///     Filter::field("age").gte(18),
/// ]);
/// ```
#[derive(Clone, Debug)]
pub struct Filter(FilterExpr);

impl Filter {
    /// Start building a filter for the given field name.
    pub fn field(field: impl Into<String>) -> FieldFilter {
        FieldFilter {
            field: field.into(),
        }
    }

    /// Combine multiple filters with AND logic.
    pub fn and(filters: impl IntoIterator<Item = Filter>) -> Self {
        let mut iter = filters.into_iter();
        let first = iter
            .next()
            .expect("Filter::and requires at least one filter");
        let mut expr = first.0;
        for f in iter {
            expr = FilterExpr::And {
                left: Box::new(expr),
                right: Box::new(f.0),
            };
        }
        Filter(expr)
    }

    /// Combine multiple filters with OR logic.
    pub fn or(filters: impl IntoIterator<Item = Filter>) -> Self {
        let mut iter = filters.into_iter();
        let first = iter
            .next()
            .expect("Filter::or requires at least one filter");
        let mut expr = first.0;
        for f in iter {
            expr = FilterExpr::Or {
                left: Box::new(expr),
                right: Box::new(f.0),
            };
        }
        Filter(expr)
    }

    /// Negate a filter.
    pub fn not(filter: Filter) -> Self {
        Filter(FilterExpr::Not {
            expr: Box::new(filter.0),
        })
    }

    /// Consume the filter and return the inner `FilterExpr`.
    pub fn build(self) -> FilterExpr {
        self.0
    }
}

/// Intermediate builder for a field-level comparison.
/// Created by [`Filter::field`].
#[derive(Clone, Debug)]
pub struct FieldFilter {
    field: String,
}

macro_rules! impl_op {
    ($method:ident, $op:ident) => {
        impl_op!($method, $op, $crate::types::DataValue);
    };
    ($method:ident, $op:ident, $convert:ty) => {
        pub fn $method(self, value: impl Into<$convert>) -> Filter {
            Filter(FilterExpr::Simple {
                field: self.field,
                operator: crate::types::Op::$op,
                value: value.into(),
            })
        }
    };
}

impl FieldFilter {
    impl_op!(eq, Eq);
    impl_op!(ne, NotEq);
    impl_op!(gt, Gt);
    impl_op!(lt, Lt);
    impl_op!(gte, GtEq);
    impl_op!(lte, LtEq);

    /// String equality (aliases `eq` for clarity).
    pub fn equals(self, value: impl Into<String>) -> Filter {
        Filter(FilterExpr::Simple {
            field: self.field,
            operator: Op::Eq,
            value: DataValue::String(value.into()),
        })
    }

    /// Substring match.
    pub fn contains(self, value: impl Into<String>) -> Filter {
        Filter(FilterExpr::Simple {
            field: self.field,
            operator: Op::Contains,
            value: crate::types::DataValue::String(value.into()),
        })
    }

    /// Prefix match.
    pub fn starts_with(self, value: impl Into<String>) -> Filter {
        Filter(FilterExpr::Simple {
            field: self.field,
            operator: Op::StartsWith,
            value: crate::types::DataValue::String(value.into()),
        })
    }

    /// Suffix match.
    pub fn ends_with(self, value: impl Into<String>) -> Filter {
        Filter(FilterExpr::Simple {
            field: self.field,
            operator: Op::EndsWith,
            value: crate::types::DataValue::String(value.into()),
        })
    }

    /// Inclusive range check: `field BETWEEN low AND high`.
    pub fn between(self, low: f64, high: f64) -> Filter {
        Filter(FilterExpr::Simple {
            field: self.field,
            operator: Op::Between,
            value: crate::types::DataValue::Array(vec![
                crate::types::DataValue::Float(OrderedFloat(low)),
                crate::types::DataValue::Float(OrderedFloat(high)),
            ]),
        })
    }

    /// Member-of check: `field IN (values)`.
    pub fn r#in<V: Into<crate::types::DataValue>>(self, values: Vec<V>) -> Filter {
        Filter(FilterExpr::Simple {
            field: self.field,
            operator: Op::In,
            value: crate::types::DataValue::Array(values.into_iter().map(|v| v.into()).collect()),
        })
    }
}

// ─── Convenience conversions ──────────────────────────────────────────

impl From<&str> for DataValue {
    fn from(s: &str) -> Self {
        DataValue::String(s.to_string())
    }
}

impl From<String> for DataValue {
    fn from(s: String) -> Self {
        DataValue::String(s)
    }
}

impl From<f64> for DataValue {
    fn from(n: f64) -> Self {
        DataValue::Float(OrderedFloat(n))
    }
}

impl From<i64> for DataValue {
    fn from(n: i64) -> Self {
        DataValue::Int(n)
    }
}

impl From<bool> for DataValue {
    fn from(b: bool) -> Self {
        DataValue::Bool(b)
    }
}

impl From<Filter> for FilterExpr {
    fn from(f: Filter) -> Self {
        f.0
    }
}

impl From<Pipeline> for Query {
    fn from(p: Pipeline) -> Self {
        p.build()
    }
}

// ─── AggregateStrategy helpers ────────────────────────────────────────

impl AggregateStrategy {
    pub const fn count() -> Self {
        AggregateStrategy::Count
    }
    pub const fn sum() -> Self {
        AggregateStrategy::Sum
    }
    pub const fn avg() -> Self {
        AggregateStrategy::Average
    }
}
