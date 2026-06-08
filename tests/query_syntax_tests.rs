use konda::executor::execute_query;
use konda::parser::parse_query;
use konda::storage::StorageEngine;
use konda::types::DataValue;

#[test]
fn test_execute_all_query_syntaxes() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_syntax_exec_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // 1. Initialize Storage Engine
    let engine =
        StorageEngine::new(&temp_dir, 1024 * 1024).expect("Failed to initialize StorageEngine");

    // 2. Populate streams with mock data
    // Ingest users
    engine
        .append(
            "users",
            "user_101",
            DataValue::String(
                r#"{"name": "Alice", "email": "alice@example.com", "status": "active"}"#
                    .to_string(),
            ),
            false,
        )
        .unwrap();
    engine
        .append(
            "users",
            "user_102",
            DataValue::String(
                r#"{"name": "Bob", "email": "bob@example.com", "status": "inactive"}"#.to_string(),
            ),
            false,
        )
        .unwrap();
    engine
        .append(
            "users",
            "user_103",
            DataValue::String(
                r#"{"name": "Charlie", "email": "charlie@example.com", "status": "active"}"#
                    .to_string(),
            ),
            false,
        )
        .unwrap();

    // Ingest logs
    engine
        .append(
            "logs",
            "log_101",
            DataValue::String(
                r#"{"status": "error", "message": "auth failure", "timestamp": 1717068000}"#
                    .to_string(),
            ),
            false,
        )
        .unwrap();
    engine
        .append(
            "logs",
            "log_102",
            DataValue::String(
                r#"{"status": "info", "message": "connection closed", "timestamp": 1717069000}"#
                    .to_string(),
            ),
            false,
        )
        .unwrap();

    // Ingest orders
    engine
        .append(
            "orders",
            "order_101",
            DataValue::String(
                r#"{"amount": 150, "status": "completed", "timestamp": 1717068000}"#.to_string(),
            ),
            false,
        )
        .unwrap();
    engine
        .append(
            "orders",
            "order_102",
            DataValue::String(
                r#"{"amount": 50, "status": "pending", "timestamp": 1717069000}"#.to_string(),
            ),
            false,
        )
        .unwrap();

    // Ingest products
    engine
        .append(
            "products",
            "prod_101",
            DataValue::String(r#"{"name": "mug", "price": 5.99}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "products",
            "prod_102",
            DataValue::String(r#"{"name": "keyboard", "price": 45.00}"#.to_string()),
            false,
        )
        .unwrap();

    // Ingest events
    engine
        .append(
            "events",
            "event_101",
            DataValue::String(r#"{"type": "click", "timestamp": 1717068000}"#.to_string()),
            false,
        )
        .unwrap();

    // Ingest inventory
    engine
        .append(
            "inventory",
            "SKU-001",
            DataValue::String(r#"{"qty": 100}"#.to_string()),
            false,
        )
        .unwrap();

    // 3. Define and run all 27 query syntaxes
    let queries = vec![
        // FETCH OPERATIONS
        "from(\"users\") | get(\"user_102\")",
        "from(\"users\") | filter(key in [\"user_102\", \"user_103\", \"user_104\"])",
        "from(\"users\")",
        "from(\"logs\") | limit(100)",
        "from(\"orders\") | filter(amount > 100)",
        "from(\"users\") | filter(key startsWith \"user_\")",
        "from(\"logs\") | filter(status == \"error\") | filter(timestamp > 1700000000)",
        "from(\"orders\") | filter(amount > 100 and status == \"completed\")",
        "from(\"users\") | map(name, email, status)",
        "from(\"users\") | count()",
        "from(\"orders\") | group(status, count(), sum(amount))",
        "from(\"orders\") | sort(timestamp, desc)",
        "from(\"users\") | page(2, 50)",
        "from(\"events\") | page(cursor(\"2026-05-30T11:42:00Z\"), 50)",
        // INSERT OPERATIONS
        "from(\"users\").insert(\"user_200\", { name: \"Dave\", email: \"dave@example.com\" })",
        "from(\"users\").insert([[\"user_203\", { name: \"Bob\" }], [\"user_204\", { name: \"Charlie\" }]])",
        "from(\"events\").insert(\"click:cta_btn:1717068000\", { userId: \"user_102\", duration_ms: 12 })",
        // UPSERT OPERATIONS
        "from(\"users\").upsert(\"user_102\", { name: \"Alice\", status: \"active\", updated: true })",
        "from(\"inventory\").upsert([[\"SKU-001\", { qty: 150 }], [\"SKU-002\", { qty: 85 }]])",
        // UPDATE OPERATIONS
        "from(\"users\").update(\"user_102\", { status: \"active\", lastLogin: 1717068000 })",
        "from(\"users\") | filter(key in [\"user_102\", \"user_103\"]) .update({ status: \"inactive\" })",
        "from(\"orders\") | filter(status == \"pending\") .update({ status: \"processing\" })",
        "from(\"products\") | filter(price < 10) .update({ price: 11, updatedAt: 1717068000 })",
        // DELETE / REMOVE OPERATIONS
        "from(\"users\").delete(\"user_102\")",
        "from(\"users\") | filter(status == \"inactive\") .delete()",
        "from(\"logs\").empty()",
        "drop(\"orders\")",
    ];

    for q_str in queries {
        println!("Executing: {}", q_str);
        let parsed = parse_query(q_str)
            .unwrap_or_else(|e| panic!("Failed to parse query '{}': {}", q_str, e));
        let results = execute_query(&engine, &parsed)
            .unwrap_or_else(|e| panic!("Failed to execute query '{}': {}", q_str, e));
        println!("Success! Result size: {}", results.len());
    }

    // Clean up temp directory
    // let _ = fs::remove_dir_all(&temp_dir);
}
