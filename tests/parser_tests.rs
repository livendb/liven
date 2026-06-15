use liven::parser::{parse_pipeline, parse_query};
use liven::types::{DataValue, FilterExpr, Op, PipelineStage, Query};

#[test]
fn test_pipeline_parsing() {
    let query = "from(\"logs\") | filter(status == \"error\") | limit(5)";
    let stages = parse_pipeline(query).unwrap();
    assert_eq!(stages.len(), 3);
    assert_eq!(
        stages[0],
        PipelineStage::From {
            stream_name: "logs".to_string()
        }
    );
    match &stages[1] {
        PipelineStage::Filter { expr } => match expr {
            FilterExpr::Simple {
                field,
                operator,
                value,
            } => {
                assert_eq!(field, "status");
                assert_eq!(operator, &Op::Eq);
                assert_eq!(value, &DataValue::String("error".to_string()));
            }
            _ => panic!("Expected simple filter expression"),
        },
        _ => panic!("Expected filter stage"),
    }
    assert_eq!(stages[2], PipelineStage::Limit { count: 5 });
}

#[test]
fn test_delete_trash_parsing() {
    let query = "from(\"logs\") | delete() | trash";
    let stages = parse_pipeline(query).unwrap();
    assert_eq!(stages.len(), 3);
    assert_eq!(stages[1], PipelineStage::Delete);
    assert_eq!(stages[2], PipelineStage::Trash);

    let query2 = "from(\"logs\") | delete | trash()";
    let stages2 = parse_pipeline(query2).unwrap();
    assert_eq!(stages2.len(), 3);
    assert_eq!(stages2[1], PipelineStage::Delete);
    assert_eq!(stages2[2], PipelineStage::Trash);
}

#[test]
fn test_all_proposed_syntaxes() {
    // 1. Fetch by key
    let q = parse_query("from(\"users\") | get(\"user_102\")").unwrap();
    assert!(matches!(q, Query::Pipeline(_)));

    // 2. Filter key in
    let q =
        parse_query("from(\"users\") | filter(key in [\"user_102\", \"user_103\", \"user_104\"])")
            .unwrap();
    if let Query::Pipeline(ref stages) = q {
        assert_eq!(stages.len(), 2);
        if let PipelineStage::Filter {
            expr:
                FilterExpr::Simple {
                    field,
                    operator,
                    value,
                },
        } = &stages[1]
        {
            assert_eq!(field, "key");
            assert_eq!(operator, &Op::In);
            assert!(matches!(value, DataValue::Array(_)));
        } else {
            panic!("failed to parse filter key in");
        }
    }

    // 3. Filter startsWith
    let q = parse_query("from(\"users\") | filter(key startsWith \"user_\")").unwrap();
    if let Query::Pipeline(ref stages) = q {
        if let PipelineStage::Filter {
            expr:
                FilterExpr::Simple {
                    field,
                    operator,
                    value,
                },
        } = &stages[1]
        {
            assert_eq!(field, "key");
            assert_eq!(operator, &Op::StartsWith);
            assert_eq!(value, &DataValue::String("user_".to_string()));
        } else {
            panic!("failed to parse filter key startsWith");
        }
    }

    // 4. Logical Operators (and)
    let q =
        parse_query("from(\"orders\") | filter(amount > 100 and status == \"completed\")").unwrap();
    if let Query::Pipeline(ref stages) = q {
        if let PipelineStage::Filter {
            expr: FilterExpr::And { left, right },
        } = &stages[1]
        {
            assert!(matches!(**left, FilterExpr::Simple { .. }));
            assert!(matches!(**right, FilterExpr::Simple { .. }));
        } else {
            panic!("failed to parse logical and");
        }
    }

    // 5. Count
    let q = parse_query("from(\"users\") | count()").unwrap();
    assert!(matches!(q, Query::Pipeline(_)));

    // 6. Group
    let q = parse_query("from(\"orders\") | group(status, count(), sum(amount))").unwrap();
    if let Query::Pipeline(ref stages) = q {
        if let PipelineStage::Group {
            field,
            aggregations,
        } = &stages[1]
        {
            assert_eq!(field, "status");
            assert_eq!(
                aggregations,
                &vec!["count()".to_string(), "sum(amount)".to_string()]
            );
        } else {
            panic!("failed to parse group");
        }
    }

    // 7. Sort
    let q = parse_query("from(\"orders\") | sort(timestamp, desc)").unwrap();
    if let Query::Pipeline(ref stages) = q {
        assert_eq!(
            stages[1],
            PipelineStage::Sort {
                field: "timestamp".to_string(),
                descending: true
            }
        );
    }

    // 8. Page Cursor
    let q = parse_query("from(\"events\") | page(cursor(\"2026-05-30T11:42:00Z\"), 50)").unwrap();
    if let Query::Pipeline(ref stages) = q {
        assert_eq!(
            stages[1],
            PipelineStage::PageCursor {
                cursor: "2026-05-30T11:42:00Z".to_string(),
                page_size: 50
            }
        );
    }

    // 9. Single insert
    let q = parse_query(
        "from(\"users\").insert(\"user_102\", { name: \"Alice\", email: \"alice@example.com\" })",
    )
    .unwrap();
    assert!(matches!(q, Query::Insert { .. }));

    // 10. Batch insert
    let q = parse_query("from(\"users\").insert([[\"user_103\", { name: \"Bob\" }], [\"user_104\", { name: \"Charlie\" }]])").unwrap();
    assert!(matches!(q, Query::InsertBatch { .. }));

    // 11. Pipeline-based update
    let q = parse_query("from(\"users\") | filter(key in [\"user_102\", \"user_103\"]) .update({ status: \"inactive\" })").unwrap();
    assert!(matches!(q, Query::PipelineUpdate { .. }));

    // 12. Pipeline-based delete
    let q = parse_query("from(\"users\") | filter(status == \"inactive\") .delete()").unwrap();
    assert!(matches!(q, Query::PipelineDelete { .. }));

    // 13. Empty stream
    let q = parse_query("from(\"logs\").empty()").unwrap();
    assert!(matches!(q, Query::Empty { .. }));

    // 14. Drop stream
    let q = parse_query("drop(\"old_metrics_2025\")").unwrap();
    assert!(matches!(q, Query::Drop { .. }));

    // 15. List streams
    let q = parse_query("streams()").unwrap();
    assert!(matches!(q, Query::ListStreams));
    let q = parse_query("streams").unwrap();
    assert!(matches!(q, Query::ListStreams));
    let q = parse_query("streams(  )").unwrap();
    assert!(matches!(q, Query::ListStreams));
}

// ── New feature: filter operators ──

#[test]
fn test_contains_operator() {
    let q = parse_query(r#"from("logs") | filter(message contains "error")"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}

#[test]
fn test_endswith_operator() {
    let q = parse_query(r#"from("files") | filter(name endsWith ".log")"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}

#[test]
fn test_between_operator() {
    let q = parse_query(r#"from("orders") | filter(amount between [100, 500])"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}

#[test]
fn test_not_operator() {
    let q = parse_query(r#"from("users") | filter(not status == "inactive")"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));

    // not with parenthesised expression
    let q =
        parse_query(r#"from("users") | filter(not (status == "inactive" or status == "deleted"))"#)
            .unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}

#[test]
fn test_pipe_in_string_literal() {
    // pipe character inside string literal should not split the pipeline
    let q = parse_query(r#"from("logs") | filter(message == "a|b")"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}

#[test]
fn test_sequence_with_parenthesised_sub_expr() {
    let q = parse_query(
        r#"from("auth") | sequence((action == "fail" or action == "error"), then: action == "block", within: 60000)"#,
    )
    .unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}

// ── New feature: distinct stage ──

#[test]
fn test_distinct_parsing() {
    // bare identifier
    let stages = parse_pipeline(r#"from("events") | distinct(type)"#).unwrap();
    assert_eq!(stages.len(), 2);
    assert_eq!(
        stages[0],
        PipelineStage::From {
            stream_name: "events".to_string()
        }
    );
    assert_eq!(
        stages[1],
        PipelineStage::Distinct {
            field: "type".to_string()
        }
    );

    // quoted field name
    let stages = parse_pipeline(r#"from("events") | distinct("type")"#).unwrap();
    assert_eq!(
        stages[1],
        PipelineStage::Distinct {
            field: "type".to_string()
        }
    );
}

// ── New feature: explain ──

#[test]
fn test_explain_parsing() {
    // explain a pipeline query (bare identifiers to avoid double-escape issue)
    let q = parse_query(r#"explain("from(orders) | filter(amount > 100)")"#).unwrap();
    assert!(matches!(q, Query::Explain { .. }));

    // explain an insert (bare identifiers)
    let q = parse_query(r#"explain("from(users).insert(k1, {name: test})")"#).unwrap();
    assert!(matches!(q, Query::Explain { .. }));

    // explain list streams
    let q = parse_query(r#"explain("streams")"#).unwrap();
    assert!(matches!(q, Query::Explain { .. }));

    // explain status
    let q = parse_query(r#"explain("status")"#).unwrap();
    assert!(matches!(q, Query::Explain { .. }));
}

// ── New feature: escape sequences in string literals ──

#[test]
fn test_escape_sequences_in_strings() {
    // escaped quote inside string
    let q = parse_query(r#"from("logs") | filter(message == "hello\"world")"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));

    // escaped backslash
    let q = parse_query(r#"from("paths") | filter(path == "c:\\users\\test")"#).unwrap();
    assert!(matches!(q, Query::Pipeline(_)));
}
