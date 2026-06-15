// Edge case parser tests for P26-P33

#[test]
fn test_pipe_inside_string_literal() {
    // P30: Verify | inside quoted strings doesn't break pipeline parsing
    let query = r#"from("events") | filter(name == "test|value")"#;
    let result = liven::parser::parse_query(query);

    assert!(result.is_ok(), "Should parse query with pipe in string");
    let parsed = result.unwrap();

    // Should be a pipeline with filter stage
    assert!(matches!(parsed, liven::types::Query::Pipeline(_)));
}

#[test]
fn test_string_escape_sequences() {
    // P32: Verify string escape sequences work correctly
    let test_cases = vec![
        (r#"filter(msg == "hello\nworld")"#, "newline"),
        (r#"filter(msg == "hello\tworld")"#, "tab"),
        (r#"filter(msg == "hello\"world")"#, "quote"),
        (r#"filter(msg == "hello\\world")"#, "backslash"),
    ];

    for (query, desc) in test_cases {
        let result = liven::parser::parse_query(query);
        assert!(result.is_ok(), "Should parse query with {} escape", desc);
    }
}

#[test]
fn test_numeric_queries_with_edge_cases() {
    // P33: Verify numeric queries handle edge cases without panicking
    // Test through public parse_query interface
    let test_cases = vec![
        (r#"filter(age == 25)"#, true),                 // Normal case
        (r#"filter(age == 999999999999999999)"#, true), // Large number
        (r#"filter(price == 123.456)"#, true),          // Decimal
        (r#"filter(code == "123")"#, true),             // String not number
    ];

    for (query_part, should_pass) in test_cases {
        let full_query = format!("from(\"test\") | {}", query_part);
        let result = liven::parser::parse_query(&full_query);

        if should_pass {
            assert!(result.is_ok(), "Should parse: {}", full_query);
        } else {
            // Even if it fails, it shouldn't panic
            assert!(result.is_err(), "Should fail gracefully: {}", full_query);
        }
    }
}
