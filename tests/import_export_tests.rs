use konda::client::KondaClient;
use konda::types::DataValue;
use std::fs;
use std::path::Path;
use std::process::Command;

struct ServerGuard(std::process::Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn test_cli_import_export_csv_and_jsonl() {
    // 1. Prepare temp directory and test files
    let test_dir = "./data_test_import_export";
    let _ = fs::remove_dir_all(test_dir);
    fs::create_dir_all(test_dir).unwrap();

    let csv_input_path = format!("{}/input.csv", test_dir);
    let csv_output_path = format!("{}/output.csv", test_dir);
    let jsonl_input_path = format!("{}/input.jsonl", test_dir);
    let jsonl_output_path = format!("{}/output.jsonl", test_dir);

    // CSV input data with headers (including id)
    let csv_data = "\
id,name,age,active,details
usr_1,Alice,30,true,\"{\"\"role\"\":\"\"admin\"\"}\"
usr_2,Bob,25,false,\"{\"\"role\"\":\"\"user\"\"}\"
";
    fs::write(&csv_input_path, csv_data).unwrap();

    // JSONL input data (with key, value nested structures)
    let jsonl_data = "\
{\"key\":\"log_1\",\"value\":{\"event\":\"click\",\"metadata\":{\"x\":100,\"y\":200}}}
{\"id\":\"log_2\",\"event\":\"hover\",\"metadata\":{\"x\":150,\"y\":250}}
";
    fs::write(&jsonl_input_path, jsonl_data).unwrap();

    // 2. Build the binary first to ensure cargo run doesn't print compilation output during test
    let build_status = Command::new("cargo")
        .args(&["build", "--bin", "kondadb"])
        .status()
        .expect("Failed to build kondadb binary");
    assert!(build_status.success());

    // Define a custom port and webui port to prevent collisions during tests
    let test_port = "45165";
    let test_webui_port = "45164";

    // Spawn background KondaDB server process
    let server_child = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .args(&["run", "--bin", "kondadb", "--", "start", "--no-ui"])
        .spawn()
        .expect("Failed to start background KondaDB server");

    // Use the RAII ServerGuard to ensure cleanup even if the test panics
    let _server_guard = ServerGuard(server_child);

    // Wait a brief moment for the server to spin up and bind to the port
    std::thread::sleep(std::time::Duration::from_millis(3000));

    // 3. Run CSV import command sandboxed using KONDA_DATA_DIR
    let import_csv_status = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .args(&[
            "run",
            "--bin",
            "kondadb",
            "--",
            "import",
            "--stream",
            "csv_stream",
            "--format",
            "csv",
            "--path",
            &csv_input_path,
        ])
        .status()
        .expect("Failed to execute CSV import command");
    assert!(import_csv_status.success());

    // 4. Run JSONL import command sandboxed using KONDA_DATA_DIR
    let import_jsonl_status = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .args(&[
            "run",
            "--bin",
            "kondadb",
            "--",
            "import",
            "--stream",
            "jsonl_stream",
            "--format",
            "jsonl",
            "--path",
            &jsonl_input_path,
        ])
        .status()
        .expect("Failed to execute JSONL import command");
    assert!(import_jsonl_status.success());

    // 5. Verify the server contains the imported data using KondaClient
    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut client = KondaClient::connect_with_auth_mode(
                &format!("127.0.0.1:{}", test_port),
                "test_client",
                "none",
            )
            .await
            .expect("Failed to connect to KondaDB server");

            let streams_records = client.query("streams()").await.unwrap();
            let streams: Vec<String> = streams_records
                .into_iter()
                .map(|r| match r.value {
                    DataValue::String(s) => s,
                    _ => r.key.to_string(),
                })
                .collect();

            println!("{:?}", streams);

            assert!(streams.contains(&"csv_stream".to_string()));
            assert!(streams.contains(&"jsonl_stream".to_string()));

            // Check CSV stream records
            let csv_records = client.query("from(\"csv_stream\")").await.unwrap();
            assert_eq!(csv_records.len(), 2);

            let first_csv = csv_records
                .iter()
                .find(|r| r.key.as_str() == "usr_1")
                .unwrap();
            if let DataValue::String(s) = &first_csv.value {
                assert!(s.contains("Alice"));
                assert!(s.contains("30"));
                assert!(s.contains("true"));
                assert!(s.contains("admin"));
            } else {
                panic!("Expected DataValue::String for csv payload");
            }

            // Check JSONL stream records
            let jsonl_records = client.query("from(\"jsonl_stream\")").await.unwrap();
            assert_eq!(jsonl_records.len(), 2);

            let first_jsonl = jsonl_records
                .iter()
                .find(|r| r.key.as_str() == "log_1")
                .unwrap();
            if let DataValue::String(s) = &first_jsonl.value {
                assert!(s.contains("click"));
                assert!(s.contains("100"));
            } else {
                panic!("Expected DataValue::String for jsonl payload");
            }

            let second_jsonl = jsonl_records
                .iter()
                .find(|r| r.key.as_str() == "log_2")
                .unwrap();
            if let DataValue::String(s) = &second_jsonl.value {
                assert!(s.contains("hover"));
                assert!(s.contains("250"));
            } else {
                panic!("Expected DataValue::String for jsonl payload");
            }
        });
    }

    // 6. Run CSV export command sandboxed using KONDA_DATA_DIR
    let export_csv_status = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .args(&[
            "run",
            "--bin",
            "kondadb",
            "--",
            "export",
            "--stream",
            "csv_stream",
            "--format",
            "csv",
            "--path",
            &csv_output_path,
        ])
        .status()
        .expect("Failed to execute CSV export command");
    assert!(export_csv_status.success());

    // 7. Run JSONL export command sandboxed using KONDA_DATA_DIR
    let export_jsonl_status = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .args(&[
            "run",
            "--bin",
            "kondadb",
            "--",
            "export",
            "--stream",
            "jsonl_stream",
            "--format",
            "jsonl",
            "--path",
            &jsonl_output_path,
        ])
        .status()
        .expect("Failed to execute JSONL export command");
    assert!(export_jsonl_status.success());

    // 8. Verify the exported file contents
    assert!(Path::new(&csv_output_path).exists());
    let csv_exported_content = fs::read_to_string(&csv_output_path).unwrap();
    assert!(csv_exported_content.contains("sequence_id"));
    assert!(csv_exported_content.contains("usr_1"));
    assert!(csv_exported_content.contains("usr_2"));

    assert!(Path::new(&jsonl_output_path).exists());
    let jsonl_exported_content = fs::read_to_string(&jsonl_output_path).unwrap();
    assert!(jsonl_exported_content.contains("log_1"));
    assert!(jsonl_exported_content.contains("log_2"));
    assert!(jsonl_exported_content.contains("click"));
    assert!(jsonl_exported_content.contains("hover"));

    // 8b. Run CSV export without --path and check for HOME/Downloads fallback
    let export_no_path_status = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .env("HOME", test_dir) // Override HOME so it defaults to test_dir/Downloads
        .args(&[
            "run",
            "--bin",
            "kondadb",
            "--",
            "export",
            "--stream",
            "csv_stream",
            "--format",
            "csv",
        ])
        .status()
        .expect("Failed to execute CSV export command with implicit path");
    assert!(export_no_path_status.success());

    // Verify it was written inside the mock downloads folder
    let downloads_dir = format!("{}/Downloads", test_dir);
    assert!(Path::new(&downloads_dir).exists());
    let paths = fs::read_dir(&downloads_dir).unwrap();
    let mut found_export = false;
    for path_entry in paths {
        let entry = path_entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.starts_with("csv_stream_") && name.ends_with(".csv") {
            found_export = true;
            let file_content = fs::read_to_string(entry.path()).unwrap();
            assert!(file_content.contains("usr_1"));
            assert!(file_content.contains("usr_2"));
        }
    }
    assert!(
        found_export,
        "Implicitly configured export was not found in Downloads!"
    );

    // 8c. Run export without --stream (all streams) and check for HOME/Downloads fallback
    let export_all_status = Command::new("cargo")
        .env("KONDA_DATA_DIR", test_dir)
        .env("KONDADB__STORAGE__DATA_DIRECTORY", test_dir)
        .env("KONDADB__SECURITY__MODE", "none")
        .env("KONDADB__SERVER__DB_PORT", test_port)
        .env("KONDADB__SERVER__WEBUI_PORT", test_webui_port)
        .env("HOME", test_dir) // Override HOME so it defaults to test_dir/Downloads
        .args(&[
            "run", "--bin", "kondadb", "--", "export", "--format", "jsonl",
        ])
        .status()
        .expect("Failed to execute export all streams command");
    assert!(export_all_status.success());

    // Verify both files were written inside the mock downloads folder
    let paths_all = fs::read_dir(&downloads_dir).unwrap();
    let mut found_csv_stream = false;
    let mut found_jsonl_stream = false;
    for path_entry in paths_all {
        let entry = path_entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.starts_with("csv_stream_") && name.ends_with(".jsonl") {
            found_csv_stream = true;
            let file_content = fs::read_to_string(entry.path()).unwrap();
            assert!(file_content.contains("usr_1"));
            assert!(file_content.contains("usr_2"));
        } else if name.starts_with("jsonl_stream_") && name.ends_with(".jsonl") {
            found_jsonl_stream = true;
            let file_content = fs::read_to_string(entry.path()).unwrap();
            assert!(file_content.contains("log_1"));
            assert!(file_content.contains("log_2"));
        }
    }
    assert!(
        found_csv_stream,
        "Default-configured csv_stream JSONL export was not found in Downloads!"
    );
    assert!(
        found_jsonl_stream,
        "Default-configured jsonl_stream JSONL export was not found in Downloads!"
    );

    // 9. Clean up
    let _ = fs::remove_dir_all(test_dir);
}
