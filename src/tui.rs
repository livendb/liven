use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use liven::codec::LivenFrame;
use liven::error::LivenError;
use liven::executor::execute_query;
use liven::parser::parse_query;
use liven::storage::StorageEngine;
use liven::types::{DataValue, Record};

enum DbConnection {
    Online {
        client: tokio::sync::Mutex<liven::client::LivenClient>,
        auth_key: Option<String>,
    },
    Offline {
        engine: StorageEngine,
    },
}

impl DbConnection {
    async fn execute(&self, query_str: &str) -> Result<Vec<Record>, LivenError> {
        match self {
            DbConnection::Online { client, .. } => {
                let mut client_guard = client.lock().await;
                client_guard.query(query_str).await.map_err(LivenError::Io)
            }
            DbConnection::Offline { engine } => {
                let parsed = parse_query(query_str)?;
                execute_query(engine, &parsed)
            }
        }
    }

    async fn list_streams(&self) -> Result<Vec<String>, String> {
        match self {
            DbConnection::Online { client, .. } => {
                let mut client_guard = client.lock().await;
                match client_guard.query("streams()").await {
                    Ok(records) => {
                        let mut names = Vec::new();
                        for r in records {
                            match r.value {
                                DataValue::String(s) => names.push(s),
                                _ => names.push(r.key.to_string()),
                            }
                        }
                        Ok(names)
                    }
                    Err(e) => Err(format!("Query failed: {}", e)),
                }
            }
            DbConnection::Offline { engine } => Ok(engine.list_streams()),
        }
    }
}

fn get_record_json_object(record: &Record) -> Option<serde_json::Map<String, serde_json::Value>> {
    match &record.value {
        DataValue::String(s) => {
            if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(s) {
                Some(map)
            } else {
                None
            }
        }
        DataValue::Binary(b) => {
            if let Ok(serde_json::Value::Object(map)) = rmp_serde::from_slice(b) {
                Some(map)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn format_value_cell(s: &str) -> String {
    if s.len() > 100 {
        format!("{}...", &s[..97])
    } else {
        s.to_string()
    }
}

fn format_cell(s: &str) -> String {
    if s.len() > 40 {
        format!("{}...", &s[..37])
    } else {
        s.to_string()
    }
}

fn format_error_card(err_msg: &str) -> Vec<String> {
    let mut output = Vec::new();
    let mut lines = Vec::new();
    for line in err_msg.lines() {
        let mut remaining = line;
        while !remaining.is_empty() {
            if remaining.len() <= 70 {
                lines.push(remaining.to_string());
                break;
            } else {
                let mut split_at = 70;
                if let Some(space_idx) = remaining[..70].rfind(' ')
                    && space_idx > 30
                {
                    split_at = space_idx;
                }
                lines.push(remaining[..split_at].to_string());
                remaining = remaining[split_at..].trim_start();
            }
        }
    }
    if lines.is_empty() {
        lines.push(err_msg.to_string());
    }

    let max_line_len = lines.iter().map(|l| l.len()).max().unwrap_or(0);
    let box_width = std::cmp::max(max_line_len + 4, 30);
    let horizontal = "─".repeat(box_width - 2);

    output.push(format!("┌{}┐", horizontal));
    let title = "ERROR";
    let title_padding_left = (box_width - 2 - title.len()) / 2;
    let title_padding_right = box_width - 2 - title.len() - title_padding_left;
    output.push(format!(
        "│{:width_l$}{}{:width_r$}│",
        "",
        title,
        "",
        width_l = title_padding_left,
        width_r = title_padding_right
    ));
    output.push(format!("├{}┤", horizontal));
    for line in lines {
        let padding = box_width - 2 - line.len() - 2; // 2 spaces padding on left
        output.push(format!("│  {}  {:padding$}│", line, "", padding = padding));
    }
    output.push(format!("└{}┘", horizontal));
    output
}

fn format_records_table(records: &[Record]) -> Vec<String> {
    let mut output = Vec::new();
    if records.is_empty() {
        output.push("(0 rows returned)".to_string());
        return output;
    }

    if records.len() == 1 && records[0].stream_name == "error" && records[0].key.as_str() == "error"
    {
        let err_msg = match &records[0].value {
            DataValue::String(s) => s.clone(),
            other => format!("{:?}", other),
        };
        return format_error_card(&err_msg);
    }

    let headers = ["seq_id".to_string(), "key".to_string(), "value".to_string()];

    let mut rows: Vec<Vec<String>> = Vec::new();
    for r in records {
        let mut row = Vec::new();
        row.push(r.sequence_id.to_string());
        row.push(r.key.to_string());

        let val_str = match &r.value {
            DataValue::Null => "NULL".to_string(),
            DataValue::Bool(b) => b.to_string(),
            DataValue::Int(i) => i.to_string(),
            DataValue::UInt(u) => u.to_string(),
            DataValue::Float(f) => f.to_string(),
            DataValue::String(s) => s.clone(),
            DataValue::Binary(b) => format!("<Binary: {} bytes>", b.len()),
            DataValue::Array(arr) => format!("{:?}", arr),
            DataValue::Object(obj) => format!("{:?}", obj),
            DataValue::Vector(v) => format!("{:?}", v),
        };
        row.push(val_str);
        rows.push(row);
    }

    let mut widths = vec![0; headers.len()];
    for col_idx in 0..headers.len() {
        let mut max_len = headers[col_idx].len();
        for row in &rows {
            let cell_len = if col_idx == 2 {
                format_value_cell(&row[col_idx]).len()
            } else {
                format_cell(&row[col_idx]).len()
            };
            if cell_len > max_len {
                max_len = cell_len;
            }
        }
        widths[col_idx] = max_len;
    }

    output.push(build_border_line('┌', '┬', '┐', &widths));

    let mut header_row = "│".to_string();
    for col_idx in 0..headers.len() {
        header_row.push_str(&format!(
            " {: <width$} │",
            headers[col_idx],
            width = widths[col_idx]
        ));
    }
    output.push(header_row);

    output.push(build_border_line('├', '┼', '┤', &widths));

    for row in rows {
        let mut row_str = "│".to_string();
        for col_idx in 0..headers.len() {
            let formatted_str = if col_idx == 2 {
                format_value_cell(&row[col_idx])
            } else {
                format_cell(&row[col_idx])
            };
            row_str.push_str(&format!(
                " {: <width$} │",
                formatted_str,
                width = widths[col_idx]
            ));
        }
        output.push(row_str);
    }

    output.push(build_border_line('└', '┴', '┘', &widths));
    output.push(format!("({} rows returned)", records.len()));
    output
}

fn build_border_line(left: char, middle: char, right: char, widths: &[usize]) -> String {
    let mut s = String::new();
    s.push(left);
    for (i, w) in widths.iter().enumerate() {
        s.push_str(&"─".repeat(w + 2));
        if i < widths.len() - 1 {
            s.push(middle);
        }
    }
    s.push(right);
    s
}

fn format_mutation_card(records: &[Record]) -> Vec<String> {
    let mut output = Vec::new();
    if records.len() == 1
        && records[0].key.as_str() == "status"
        && let Some(map) = get_record_json_object(&records[0])
    {
        let status = map
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("success");
        let affected = map
            .get("affected_rows")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let stream = &records[0].stream_name;

        output.push("┌────────────────────────────────────────────────────────┐".to_string());
        output.push("│                 TRANSACTION COMMITTED                  │".to_string());
        output.push("├────────────────────────────────────────────────────────┤".to_string());
        output.push(format!("│  Stream Name: {: <41}│", stream));
        output.push(format!("│  Status:      {: <41}│", status));
        output.push(format!("│  Rows Mutated:{: <41}│", affected));
        output.push("└────────────────────────────────────────────────────────┘".to_string());
        return output;
    }
    format_records_table(records)
}

struct TerminalRestorer;

impl Drop for TerminalRestorer {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    }
}

fn insert_char_at(s: &mut String, idx: usize, c: char) {
    let mut result = String::new();
    let mut char_inserted = false;
    for (i, ch) in s.chars().enumerate() {
        if i == idx {
            result.push(c);
            char_inserted = true;
        }
        result.push(ch);
    }
    if !char_inserted {
        result.push(c);
    }
    *s = result;
}

fn remove_char_at(s: &mut String, idx: usize) {
    if idx == 0 {
        return;
    }
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i != idx - 1 {
            result.push(ch);
        }
    }
    *s = result;
}

enum ShellEvent {
    QueryResult {
        query: String,
        duration: Duration,
        output: Vec<String>,
    },
    QueryError {
        query: String,
        err: String,
    },
    TailRecord(Record),
    TailStatus(String),
    TailError(String),
}

pub async fn run_shell(auth_key: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let app_config = liven::config::AppConfig::load()?;
    let db_addr = format!("{}:{}", app_config.server.host, app_config.server.db_port);
    let client_res = if let Some(ref key) = auth_key {
        let full_addr = format!("{}?auth_key={}", db_addr, key);
        liven::client::LivenClient::connect_with_id(&full_addr, "default_client").await
    } else {
        liven::client::LivenClient::connect(&db_addr).await
    };
    let conn = match client_res {
        Ok(client) => DbConnection::Online {
            client: tokio::sync::Mutex::new(client),
            auth_key: auth_key.clone(),
        },
        Err(e) => {
            if auth_key.is_some() || crate::cli::is_server_running() {
                return Err(format!(
                    "Connection/Authentication failed for LIVEN server at {}: {}",
                    db_addr, e
                )
                .into());
            }
            let max_segment_size = app_config.storage.max_segment_size_mb * 1024 * 1024;
            let data_dir = app_config.storage.data_directory.clone();
            let engine = StorageEngine::new(&data_dir, max_segment_size as u64).map_err(|err| {
                format!(
                    "Failed to load local StorageEngine: {}. (Original connection error: {})",
                    err, e
                )
            })?;
            DbConnection::Offline { engine }
        }
    };
    let is_online = matches!(conn, DbConnection::Online { .. });

    let conn = Arc::new(conn);

    // Securely transition terminal to Raw Mode & Alternate Screen
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    // RAII Restorer ensures terminal state is restored on loop break/panic
    let _restorer = TerminalRestorer;

    // Set panic hook to restore terminal
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        default_hook(info);
    }));

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut input_buffer = String::new();
    let mut temp_saved_buffer = String::new();
    let mut cursor_position: usize = 0;
    let mut command_history: Vec<String> = Vec::new();
    let mut history_index: Option<usize> = None;

    let mut logs: Vec<String> = Vec::new();
    let mut logs_scroll: usize = 0;
    let mut logs_height: usize = 10;

    // Standard Welcome Banner
    logs.push(
        "┌───────────────────────────────────────────────────────────────────────────┐".to_string(),
    );
    logs.push(
        "│                    LIVEN INTERACTIVE QUERY CONSOLE                        │".to_string(),
    );
    logs.push(
        "├───────────────────────────────────────────────────────────────────────────┤".to_string(),
    );
    if is_online {
        let status_str = format!(
            "│  ● Status: ONLINE | Connected to active server at {}",
            db_addr
        );
        logs.push(format!("{:<76}│", status_str));
    } else {
        let status_str = format!(
            "│  ● Status: OFFLINE | Initialized direct StorageEngine on {}",
            app_config.storage.data_directory
        );
        logs.push(format!("{:<76}│", status_str));
    }
    logs.push(
        "│                                                                           │".to_string(),
    );
    logs.push(
        "│  Type \\help for quick usage and syntax guide.                             │"
            .to_string(),
    );
    logs.push(
        "│  Type \\q, exit, or quit to close the console.                            │".to_string(),
    );
    logs.push(
        "└───────────────────────────────────────────────────────────────────────────┘".to_string(),
    );
    logs.push(String::new());

    let (shell_tx, mut shell_rx) = tokio::sync::mpsc::unbounded_channel::<ShellEvent>();

    let mut interval = tokio::time::interval(Duration::from_millis(15));

    loop {
        interval.tick().await;

        // Process User Inputs asynchronously without blocking
        while event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                match key.code {
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        // Ctrl+C aborts terminal
                        return Ok(());
                    }
                    KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        // Ctrl+D acts like Exit
                        return Ok(());
                    }
                    KeyCode::Char(c) => {
                        insert_char_at(&mut input_buffer, cursor_position, c);
                        cursor_position += 1;
                    }
                    KeyCode::Backspace => {
                        if cursor_position > 0 {
                            remove_char_at(&mut input_buffer, cursor_position);
                            cursor_position -= 1;
                        }
                    }
                    KeyCode::Delete => {
                        if cursor_position < input_buffer.chars().count() {
                            remove_char_at(&mut input_buffer, cursor_position + 1);
                        }
                    }
                    KeyCode::Left => {
                        cursor_position = cursor_position.saturating_sub(1);
                    }
                    KeyCode::Right => {
                        cursor_position =
                            std::cmp::min(cursor_position + 1, input_buffer.chars().count());
                    }
                    KeyCode::Up => {
                        if !command_history.is_empty() {
                            if history_index.is_none() {
                                temp_saved_buffer = input_buffer.clone();
                                history_index = Some(command_history.len() - 1);
                            } else {
                                let idx = history_index.unwrap();
                                if idx > 0 {
                                    history_index = Some(idx - 1);
                                }
                            }
                            if let Some(idx) = history_index {
                                input_buffer = command_history[idx].clone();
                                cursor_position = input_buffer.chars().count();
                            }
                        }
                    }
                    KeyCode::Down => {
                        if let Some(idx) = history_index {
                            if idx + 1 < command_history.len() {
                                history_index = Some(idx + 1);
                                input_buffer = command_history[idx + 1].clone();
                            } else {
                                history_index = None;
                                input_buffer = temp_saved_buffer.clone();
                            }
                            cursor_position = input_buffer.chars().count();
                        }
                    }
                    KeyCode::PageUp => {
                        logs_scroll = logs_scroll.saturating_sub(5);
                    }
                    KeyCode::PageDown => {
                        logs_scroll =
                            std::cmp::min(logs_scroll + 5, logs.len().saturating_sub(logs_height));
                    }
                    KeyCode::Home => {
                        cursor_position = 0;
                    }
                    KeyCode::End => {
                        cursor_position = input_buffer.chars().count();
                    }
                    KeyCode::Enter => {
                        let query_trimmed = input_buffer.trim().to_string();
                        if !query_trimmed.is_empty() {
                            command_history.push(input_buffer.clone());
                            history_index = None;
                            input_buffer.clear();
                            cursor_position = 0;

                            let query_str = query_trimmed.clone();
                            if query_str == "exit" || query_str == "quit" || query_str == "\\q" {
                                return Ok(());
                            }

                            let conn_clone = conn.clone();
                            let tx = shell_tx.clone();

                            // Spawn standard/tail operations non-blockingly
                            tokio::spawn(async move {
                                let query_trimmed = query_str.trim();
                                if query_trimmed == "\\help" {
                                    let mut help_lines = Vec::new();
                                    help_lines.push("─────────────────────────────────────────────────────────────────────────────".to_string());
                                    help_lines.push("                            LIVEN TUI QUICK HELP GUIDE                       ".to_string());
                                    help_lines.push("─────────────────────────────────────────────────────────────────────────────".to_string());
                                    help_lines.push(
                                        "  \\d, \\dt          List all active streams (tables)"
                                            .to_string(),
                                    );
                                    help_lines.push("  \\d <stream>      Describe schema/fields and record count for a stream".to_string());
                                    help_lines.push(
                                        "  \\help            Show this help guide".to_string(),
                                    );
                                    help_lines.push(
                                        "  \\q, exit, quit  Exit the terminal shell".to_string(),
                                    );
                                    help_lines.push("".to_string());
                                    help_lines.push("  Operational Query Examples:".to_string());
                                    help_lines.push("    -- Ingest / Insert records".to_string());
                                    help_lines.push("    from(\"users\").insert(\"user_100\", { \"name\": \"Alice\", \"role\": \"admin\" });".to_string());
                                    help_lines.push("".to_string());
                                    help_lines.push(
                                        "    -- Query pipelines with forward pipe combinator"
                                            .to_string(),
                                    );
                                    help_lines.push("    from(\"users\") | filter(role == \"admin\") | limit(5);".to_string());
                                    help_lines.push("".to_string());
                                    help_lines
                                        .push("    -- Mutate / Update matched records".to_string());
                                    help_lines.push("    from(\"users\") | filter(key == \"user_100\") .update({ \"status\": \"active\" });".to_string());
                                    help_lines.push("".to_string());
                                    help_lines.push("    -- Drop streams completely".to_string());
                                    help_lines.push("    drop(\"users\");".to_string());
                                    help_lines.push("─────────────────────────────────────────────────────────────────────────────".to_string());

                                    let _ = tx.send(ShellEvent::QueryResult {
                                        query: "\\help".to_string(),
                                        duration: Duration::from_secs(0),
                                        output: help_lines,
                                    });
                                    return;
                                }

                                if query_trimmed == "\\d" || query_trimmed == "\\dt" {
                                    match conn_clone.list_streams().await {
                                        Ok(streams) => {
                                            let mut output = Vec::new();
                                            if streams.is_empty() {
                                                output.push(
                                                    "No streams active in the database."
                                                        .to_string(),
                                                );
                                            } else {
                                                output.push(
                                                    "┌─────────────────────────────────────────┐"
                                                        .to_string(),
                                                );
                                                output.push(
                                                    "│                 STREAMS                 │"
                                                        .to_string(),
                                                );
                                                output.push(
                                                    "├─────────────────────────────────────────┤"
                                                        .to_string(),
                                                );
                                                for s in streams {
                                                    output.push(format!("│  {: <39}│", s));
                                                }
                                                output.push(
                                                    "└─────────────────────────────────────────┘"
                                                        .to_string(),
                                                );
                                            }
                                            let _ = tx.send(ShellEvent::QueryResult {
                                                query: query_trimmed.to_string(),
                                                duration: Duration::from_secs(0),
                                                output,
                                            });
                                        }
                                        Err(e) => {
                                            let _ = tx.send(ShellEvent::QueryError {
                                                query: query_trimmed.to_string(),
                                                err: format!("Error listing streams: {}", e),
                                            });
                                        }
                                    }
                                    return;
                                }

                                if query_trimmed.starts_with("\\d ") {
                                    let parts: Vec<&str> =
                                        query_trimmed.split_whitespace().collect();
                                    if parts.len() == 2 {
                                        let stream_name = parts[1].to_string();
                                        let conn_for_desc = conn_clone.clone();

                                        let streams = match conn_for_desc.list_streams().await {
                                            Ok(s) => s,
                                            Err(e) => {
                                                let _ = tx.send(ShellEvent::QueryError {
                                                    query: query_trimmed.to_string(),
                                                    err: format!("Error checking streams: {}", e),
                                                });
                                                return;
                                            }
                                        };

                                        if !streams.contains(&stream_name) {
                                            let _ = tx.send(ShellEvent::QueryError {
                                                query: query_trimmed.to_string(),
                                                err: format!(
                                                    "Error: Stream '{}' does not exist.",
                                                    stream_name
                                                ),
                                            });
                                            return;
                                        }

                                        let count_query =
                                            format!("from(\"{}\") | count();", stream_name);
                                        let count = match conn_for_desc.execute(&count_query).await
                                        {
                                            Ok(recs) => {
                                                if let Some(r) = recs.first() {
                                                    match &r.value {
                                                        DataValue::UInt(u) => *u,
                                                        _ => 0,
                                                    }
                                                } else {
                                                    0
                                                }
                                            }
                                            Err(_) => 0,
                                        };

                                        let sample_query =
                                            format!("from(\"{}\") | limit(1);", stream_name);
                                        let sample_recs = conn_for_desc
                                            .execute(&sample_query)
                                            .await
                                            .unwrap_or_default();

                                        let mut output = Vec::new();
                                        output.push(format!("Stream Information: {}", stream_name));
                                        output.push(
                                            "───────────────────────────────────────────────"
                                                .to_string(),
                                        );
                                        output.push(format!("  Approximate Records: {}", count));

                                        if let Some(sample) = sample_recs.first() {
                                            output.push("  System Fields:".to_string());
                                            output.push("    - sequence_id: UInt".to_string());
                                            output.push("    - timestamp: Int".to_string());
                                            output.push(format!(
                                                "    - key: String (Value: \"{}\")",
                                                sample.key
                                            ));

                                            if let Some(map) = get_record_json_object(sample) {
                                                output
                                                    .push("  Document Payload Fields:".to_string());
                                                for (k, v) in map {
                                                    let ty = match v {
                                                        serde_json::Value::Null => "Null",
                                                        serde_json::Value::Bool(_) => "Bool",
                                                        serde_json::Value::Number(_) => "Number",
                                                        serde_json::Value::String(_) => "String",
                                                        serde_json::Value::Array(_) => "Array",
                                                        serde_json::Value::Object(_) => "Object",
                                                    };
                                                    let mut sample_val = v.to_string();
                                                    if sample_val.len() > 30 {
                                                        sample_val =
                                                            format!("{}...", &sample_val[..27]);
                                                    }
                                                    output.push(format!(
                                                        "    - {: <14}: {} (Sample: {})",
                                                        k, ty, sample_val
                                                    ));
                                                }
                                            } else {
                                                output.push("  Value Payload:".to_string());
                                                output.push(format!(
                                                    "    - value: Primitive ({:?})",
                                                    sample.value
                                                ));
                                            }
                                        } else {
                                            output.push("  Status: Empty Stream (No sample payload available)".to_string());
                                        }
                                        output.push(
                                            "───────────────────────────────────────────────"
                                                .to_string(),
                                        );

                                        let _ = tx.send(ShellEvent::QueryResult {
                                            query: query_trimmed.to_string(),
                                            duration: Duration::from_secs(0),
                                            output,
                                        });
                                    } else {
                                        let _ = tx.send(ShellEvent::QueryError {
                                            query: query_trimmed.to_string(),
                                            err: "Usage: \\d <stream_name>".to_string(),
                                        });
                                    }
                                    return;
                                }

                                let is_tail = (query_trimmed.starts_with("tail(\"")
                                    && query_trimmed.ends_with("\")"))
                                    || (query_trimmed.starts_with("tail('")
                                        && query_trimmed.ends_with("')"));

                                if is_tail {
                                    match &*conn_clone {
                                        DbConnection::Online { auth_key, .. } => {
                                            let stream_name =
                                                if query_trimmed.starts_with("tail(\"") {
                                                    query_trimmed["tail(\"".len()
                                                        ..query_trimmed.len() - "\")".len()]
                                                        .to_string()
                                                } else {
                                                    query_trimmed["tail('".len()
                                                        ..query_trimmed.len() - "')".len()]
                                                        .to_string()
                                                };

                                            let _ = tx.send(ShellEvent::TailStatus(format!(
                                                "Subscribing to live stream '{}'...",
                                                stream_name
                                            )));

                                            let db_addr =
                                                if let Ok(cfg) = liven::config::AppConfig::load() {
                                                    format!(
                                                        "{}:{}",
                                                        cfg.server.host, cfg.server.db_port
                                                    )
                                                } else {
                                                    "127.0.0.1:43121".to_string()
                                                };

                                            let client_res = if let Some(key) = auth_key {
                                                let full_addr =
                                                    format!("{}?auth_key={}", db_addr, key);
                                                liven::client::LivenClient::connect_with_id(
                                                    &full_addr,
                                                    "default_client",
                                                )
                                                .await
                                            } else {
                                                liven::client::LivenClient::connect(&db_addr).await
                                            };

                                            match client_res {
                                                Ok(client) => {
                                                    let mut framed = client.into_inner();
                                                    let tail_query =
                                                        format!("tail(\"{}\")", stream_name);
                                                    if let Err(e) = framed.send(tail_query).await {
                                                        let _ = tx.send(ShellEvent::TailError(
                                                            format!("Failed to start tail: {}", e),
                                                        ));
                                                        return;
                                                    }

                                                    while let Some(res) = framed.next().await {
                                                        match res {
                                                            Ok(LivenFrame::Records(records)) => {
                                                                for r in records {
                                                                    let _ = tx.send(
                                                                        ShellEvent::TailRecord(r),
                                                                    );
                                                                }
                                                            }
                                                            Ok(other) => {
                                                                let _ = tx.send(
                                                                    ShellEvent::TailError(format!(
                                                                        "Unexpected frame: {:?}",
                                                                        other
                                                                    )),
                                                                );
                                                                break;
                                                            }
                                                            Err(e) => {
                                                                let _ = tx.send(
                                                                    ShellEvent::TailError(format!(
                                                                        "Tail stream error: {}",
                                                                        e
                                                                    )),
                                                                );
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    let _ = tx.send(ShellEvent::TailError(
                                                        format!("Tail connection failed: {}", e),
                                                    ));
                                                }
                                            }
                                        }
                                        DbConnection::Offline { .. } => {
                                            let _ = tx.send(ShellEvent::TailError("Tail subscriptions are only supported in Online mode.".to_string()));
                                        }
                                    }
                                    return;
                                }

                                let start_time = Instant::now();
                                match conn_clone.execute(&query_str).await {
                                    Ok(records) => {
                                        let duration = start_time.elapsed();
                                        let is_mutation = query_str.contains(".insert")
                                            || query_str.contains(".update")
                                            || query_str.contains(".upsert")
                                            || query_str.contains(".delete")
                                            || query_str.contains(".empty")
                                            || query_str.starts_with("drop");

                                        let output = if is_mutation {
                                            format_mutation_card(&records)
                                        } else {
                                            format_records_table(&records)
                                        };
                                        let _ = tx.send(ShellEvent::QueryResult {
                                            query: query_str,
                                            duration,
                                            output,
                                        });
                                    }
                                    Err(e) => {
                                        let _ = tx.send(ShellEvent::QueryError {
                                            query: query_str,
                                            err: e.to_string(),
                                        });
                                    }
                                }
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        // Process Query/Stream results on-the-fly and output them beautifully
        while let Ok(event) = shell_rx.try_recv() {
            match event {
                ShellEvent::QueryResult {
                    query,
                    duration,
                    output,
                } => {
                    logs.push(format!("liven=> {};", query));
                    for line in output {
                        logs.push(line);
                    }
                    logs.push(format!("Execution duration: {:.2?}", duration));
                    logs.push(String::new());
                }
                ShellEvent::QueryError { query, err } => {
                    logs.push(format!("liven=> {};", query));
                    for line in format_error_card(&err) {
                        logs.push(line);
                    }
                    logs.push(String::new());
                }
                ShellEvent::TailRecord(r) => {
                    let val_str = match &r.value {
                        DataValue::Null => "NULL".to_string(),
                        DataValue::Bool(b) => b.to_string(),
                        DataValue::Int(i) => i.to_string(),
                        DataValue::UInt(u) => u.to_string(),
                        DataValue::Float(f) => f.to_string(),
                        DataValue::String(s) => s.clone(),
                        DataValue::Binary(b) => format!("<Binary: {} bytes>", b.len()),
                        DataValue::Array(arr) => format!("{:?}", arr),
                        DataValue::Object(obj) => format!("{:?}", obj),
                        DataValue::Vector(v) => format!("{:?}", v),
                    };
                    logs.push(format!(
                        " [tail] Seq: #{} | Key: {} | Value: {}",
                        r.sequence_id, r.key, val_str
                    ));
                }
                ShellEvent::TailStatus(msg) => {
                    logs.push(format!(" [status] {}", msg));
                }
                ShellEvent::TailError(err) => {
                    logs.push(format!(" [error] {}", err));
                }
            }
            logs_scroll = logs.len().saturating_sub(logs_height);
        }

        // Render standard layout components
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(5),
                    Constraint::Length(3),
                ])
                .split(f.size());

            let status_style = if is_online {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            };
            let mode_text = if is_online {
                "ONLINE (Active Server)"
            } else {
                "OFFLINE (Local Store)"
            };

            let header_block = Block::default()
                .borders(Borders::ALL)
                .title(" System Status ")
                .title_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .border_style(Style::default().fg(Color::DarkGray));
            let (host, port) = if let Ok(cfg) = liven::config::AppConfig::load() {
                (cfg.server.host, cfg.server.db_port)
            } else {
                ("127.0.0.1".to_string(), 43121)
            };

            let header_paragraph = Paragraph::new(Line::from(vec![
                Span::raw(" LivenDB STREAM-FIRST DATABASE | Mode: "),
                Span::styled(mode_text, status_style),
                Span::raw(format!(" | Host: {} | Port: {}", host, port)),
            ]))
            .block(header_block);
            f.render_widget(header_paragraph, chunks[0]);

            let log_area = chunks[1];
            logs_height = (log_area.height as usize).saturating_sub(2);

            let log_block = Block::default()
                .borders(Borders::ALL)
                .title(" Query Feed & Live Streams (PageUp/PageDown to scroll) ")
                .title_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .border_style(Style::default().fg(Color::DarkGray));

            let visible_logs: Vec<ListItem> = logs
                .iter()
                .skip(logs_scroll)
                .take(logs_height)
                .map(|line| {
                    let clean = line
                        .replace("\x1b[31m", "")
                        .replace("\x1b[1;31m", "")
                        .replace("\x1b[32m", "")
                        .replace("\x1b[1m", "")
                        .replace("\x1b[36m", "")
                        .replace("\x1b[35m", "")
                        .replace("\x1b[38;5;244m", "")
                        .replace("\x1b[1;38;5;45m", "")
                        .replace("\x1b[0m", "");

                    if line.contains("ERROR") || line.contains("[error]") {
                        ListItem::new(Line::from(vec![Span::styled(
                            clean,
                            Style::default().fg(Color::Red),
                        )]))
                    } else if line.contains("TRANSACTION COMMITTED")
                        || line.contains("returned)")
                        || line.contains("[tail]")
                    {
                        ListItem::new(Line::from(vec![Span::styled(
                            clean,
                            Style::default().fg(Color::Green),
                        )]))
                    } else if line.contains("[status]") || line.contains("⚡") {
                        ListItem::new(Line::from(vec![Span::styled(
                            clean,
                            Style::default().fg(Color::Cyan),
                        )]))
                    } else if line.contains("liven=>") {
                        ListItem::new(Line::from(vec![Span::styled(
                            clean,
                            Style::default().fg(Color::Magenta),
                        )]))
                    } else {
                        ListItem::new(Line::from(vec![Span::raw(clean)]))
                    }
                })
                .collect();

            let list_widget = List::new(visible_logs).block(log_block);
            f.render_widget(list_widget, log_area);

            let input_area = chunks[2];
            let max_input_width = (input_area.width as usize).saturating_sub(17); // " liven=> " is 11 chars + borders
            let display_str: String = if input_buffer.chars().count() > max_input_width {
                let start_idx = input_buffer.chars().count().saturating_sub(max_input_width);
                input_buffer.chars().skip(start_idx).collect()
            } else {
                input_buffer.clone()
            };

            let input_block = Block::default()
                .borders(Borders::ALL)
                .title(" Query Console (Type query and press Enter) ")
                .title_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .border_style(Style::default().fg(Color::DarkGray));

            let input_paragraph = Paragraph::new(Line::from(vec![
                Span::styled(
                    " liven=> ",
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(display_str),
            ]))
            .block(input_block);
            f.render_widget(input_paragraph, input_area);

            let display_cursor_pos = if input_buffer.chars().count() > max_input_width {
                max_input_width
            } else {
                cursor_position
            };
            let cursor_x = input_area.x + 12 + display_cursor_pos as u16;
            let cursor_y = input_area.y + 1;
            f.set_cursor(cursor_x, cursor_y);
        })?;
    }
}
