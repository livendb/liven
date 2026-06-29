use crate::codec::{LivenCodec, LivenFrame};
use crate::types::Record;
use futures_util::{SinkExt, StreamExt};
use std::io;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub struct LivenClient {
    framed: Framed<TcpStream, LivenCodec>,
}

impl LivenClient {
    /// Connects to a LIVEN server instance over the native wire protocol.
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let client_id = "default_client".to_string();
        Self::connect_with_id(addr, &client_id).await
    }

    /// Connects to a LIVEN server instance with a specific client ID for auth.
    pub async fn connect_with_id(addr: &str, client_id: &str) -> io::Result<Self> {
        let mode = if let Ok(config) = crate::config::AppConfig::load() {
            config.security.mode.clone()
        } else {
            "none".to_string()
        };
        Self::connect_with_auth_mode(addr, client_id, &mode).await
    }

    /// Connects to a LIVEN server instance with an explicit client ID and security mode.
    pub async fn connect_with_auth_mode(
        addr: &str,
        client_id: &str,
        mode: &str,
    ) -> io::Result<Self> {
        let stripped_scheme = if addr.starts_with("liven://") {
            &addr["liven://".len()..]
        } else {
            addr
        };

        let mut clean_addr = stripped_scheme;
        let mut parsed_auth_key = None;

        if let Some(pos) = stripped_scheme.find('?') {
            clean_addr = &stripped_scheme[..pos];
            let query_str = &stripped_scheme[pos + 1..];
            for pair in query_str.split('&') {
                let parts: Vec<&str> = pair.split('=').collect();
                if parts.len() == 2 && parts[0] == "auth_key" {
                    parsed_auth_key = Some(parts[1].to_string());
                }
            }
        }

        let tcp_stream = TcpStream::connect(clean_addr).await?;
        tcp_stream.set_nodelay(true)?;

        // Wrap with our client-configured LivenCodec (is_client: true)
        let mut framed = Framed::new(tcp_stream, LivenCodec::new(true));

        let do_auth = mode == "auth_key" || parsed_auth_key.is_some();

        if do_auth {
            let token_to_send = if let Some(key) = parsed_auth_key {
                key
            } else {
                client_id.to_string()
            };

            // 1. Send Connect frame containing the symmetric token and protocol version
            framed
                .send(LivenFrame::Connect {
                    client_id: token_to_send,
                    protocol_version: Some(1),
                })
                .await?;

            // 2. Expect Ok or Err frame
            match framed.next().await {
                Some(Ok(LivenFrame::Ok)) => {}
                Some(Ok(LivenFrame::Err(e))) => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("Authentication failed: {}", e),
                    ));
                }
                Some(Ok(other)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Expected Ok/Err frame, got: {:?}", other),
                    ));
                }
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Connection closed by server during symmetric handshake",
                    ));
                }
            }
        }

        Ok(Self { framed })
    }

    pub fn into_inner(self) -> Framed<TcpStream, LivenCodec> {
        self.framed
    }

    /// Submits a typed `Query` over the wire by serializing it to a DSL string.
    pub async fn run(&mut self, query: &crate::types::Query) -> io::Result<Vec<Record>> {
        let query_str = query.to_dsl_string();
        self.query(&query_str).await
    }

    // ── Convenience methods ────────────────────────────────────────────

    /// Insert a single record into a stream.
    pub async fn insert(
        &mut self,
        stream_name: &str,
        key: &str,
        value: serde_json::Value,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Insert {
            stream_name: stream_name.to_string(),
            key: key.to_string(),
            value,
        })
        .await
    }

    /// Upsert a record (insert or replace).
    pub async fn upsert(
        &mut self,
        stream_name: &str,
        key: &str,
        value: serde_json::Value,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Upsert {
            stream_name: stream_name.to_string(),
            key: key.to_string(),
            value,
        })
        .await
    }

    /// Update specific fields on an existing record (merge).
    pub async fn update(
        &mut self,
        stream_name: &str,
        key: &str,
        value: serde_json::Value,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Update {
            stream_name: stream_name.to_string(),
            key: key.to_string(),
            value,
        })
        .await
    }

    /// Delete a single record by key.
    pub async fn delete(&mut self, stream_name: &str, key: &str) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::DeleteKey {
            stream_name: stream_name.to_string(),
            key: key.to_string(),
        })
        .await
    }

    /// Get a single record by key.
    pub async fn get(&mut self, stream_name: &str, key: &str) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Get {
                key: key.to_string(),
            },
        ]))
        .await
    }

    /// Clear all records from a stream without removing it.
    pub async fn clear(&mut self, stream_name: &str) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Empty {
            stream_name: stream_name.to_string(),
        })
        .await
    }

    /// Drop a stream and all its data.
    pub async fn drop_stream(&mut self, stream_name: &str) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Drop {
            stream_name: stream_name.to_string(),
        })
        .await
    }

    /// Insert multiple records in one batch.
    pub async fn insert_many(
        &mut self,
        stream_name: &str,
        batch: Vec<(String, serde_json::Value)>,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::InsertBatch {
            stream_name: stream_name.to_string(),
            batch,
        })
        .await
    }

    /// Upsert multiple records in one batch.
    pub async fn upsert_many(
        &mut self,
        stream_name: &str,
        batch: Vec<(String, serde_json::Value)>,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::UpsertBatch {
            stream_name: stream_name.to_string(),
            batch,
        })
        .await
    }

    /// List all streams.
    pub async fn streams(&mut self) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::ListStreams).await
    }

    /// Get server status.
    pub async fn status(&mut self) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Status).await
    }

    /// Explain the execution plan of a query without running it.
    pub async fn explain(&mut self, query: &crate::types::Query) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Explain {
            inner_query: Box::new(query.clone()),
        })
        .await
    }

    // ── Pipeline convenience methods ───────────────────────────────────

    /// Filter records in a stream by a condition.
    pub async fn filter(
        &mut self,
        stream_name: &str,
        filter: crate::query::Filter,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Filter {
                expr: filter.build(),
            },
        ]))
        .await
    }

    /// Limit the number of results from a stream.
    pub async fn limit(&mut self, stream_name: &str, count: usize) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Limit { count },
        ]))
        .await
    }

    /// Count records in a stream.
    pub async fn count(&mut self, stream_name: &str) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Count,
        ]))
        .await
    }

    /// Sort records by a field.
    pub async fn sort(
        &mut self,
        stream_name: &str,
        field: &str,
        descending: bool,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Sort {
                field: field.to_string(),
                descending,
            },
        ]))
        .await
    }

    /// Paginate through results.
    pub async fn page(
        &mut self,
        stream_name: &str,
        page_number: usize,
        page_size: usize,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Page {
                page_number,
                page_size,
            },
        ]))
        .await
    }

    /// Project specific fields from a stream.
    pub async fn map(&mut self, stream_name: &str, fields: Vec<String>) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Map {
                projections: fields,
            },
        ]))
        .await
    }

    /// Time-windowed aggregation.
    pub async fn window(
        &mut self,
        stream_name: &str,
        duration_ms: u64,
        strategy: crate::types::AggregateStrategy,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Window {
                duration_ms,
                strategy,
            },
        ]))
        .await
    }

    /// Group records by a field with aggregations.
    pub async fn group(
        &mut self,
        stream_name: &str,
        field: &str,
        aggregations: Vec<String>,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Group {
                field: field.to_string(),
                aggregations,
            },
        ]))
        .await
    }

    /// Vector similarity search.
    pub async fn vector_filter(
        &mut self,
        stream_name: &str,
        field: &str,
        query_vector: Vec<i8>,
        threshold: f64,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::VectorFilter {
                field: field.to_string(),
                query_vector,
                threshold: ordered_float::OrderedFloat(threshold),
            },
        ]))
        .await
    }

    /// Enrich records with a left join from another stream.
    pub async fn enrich(
        &mut self,
        stream_name: &str,
        source_stream: &str,
        join_key: &str,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Enrich {
                source_stream: source_stream.to_string(),
                join_key: join_key.to_string(),
            },
        ]))
        .await
    }

    /// Correlate events within a time window.
    pub async fn correlate(
        &mut self,
        stream_name: &str,
        source_stream: &str,
        join_key: &str,
        within_ms: u64,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Correlate {
                source_stream: source_stream.to_string(),
                join_key: join_key.to_string(),
                within_ms,
            },
        ]))
        .await
    }

    /// Chain (multi-hop join) across streams.
    pub async fn chain(
        &mut self,
        stream_name: &str,
        target_stream: &str,
        join_key: &str,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Chain {
                target_stream: target_stream.to_string(),
                join_key: join_key.to_string(),
            },
        ]))
        .await
    }

    /// Sequence (ordered event pattern detection) within a time window.
    pub async fn sequence(
        &mut self,
        stream_name: &str,
        steps: Vec<crate::query::Filter>,
        within_ms: u64,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Sequence {
                steps: steps.into_iter().map(|f| f.build()).collect(),
                within_ms,
            },
        ]))
        .await
    }

    /// Deduplicate records by a specific field.
    pub async fn distinct(&mut self, stream_name: &str, field: &str) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::Distinct {
                field: field.to_string(),
            },
        ]))
        .await
    }

    /// Cursor-based pagination.
    pub async fn page_cursor(
        &mut self,
        stream_name: &str,
        cursor: &str,
        page_size: usize,
    ) -> io::Result<Vec<Record>> {
        self.run(&crate::types::Query::Pipeline(vec![
            crate::types::PipelineStage::From {
                stream_name: stream_name.to_string(),
            },
            crate::types::PipelineStage::PageCursor {
                cursor: cursor.to_string(),
                page_size,
            },
        ]))
        .await
    }

    /// Pipeline update: filter then update matching records.
    pub async fn pipeline_update(
        &mut self,
        pipeline: crate::query::Pipeline,
        update_value: serde_json::Value,
    ) -> io::Result<Vec<Record>> {
        self.run(&pipeline.build_update(update_value)).await
    }

    /// Pipeline delete: filter then delete matching records.
    pub async fn pipeline_delete(
        &mut self,
        pipeline: crate::query::Pipeline,
    ) -> io::Result<Vec<Record>> {
        self.run(&pipeline.build_delete()).await
    }

    /// Submits a query expression over the wire and awaits deserialized Records response.
    pub async fn query(&mut self, query_str: &str) -> io::Result<Vec<Record>> {
        self.framed
            .send(LivenFrame::Query(query_str.to_string()))
            .await?;

        match self.framed.next().await {
            Some(Ok(LivenFrame::Records(records))) => Ok(records),
            Some(Ok(other)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unexpected response frame from server: {:?}", other),
            )),
            Some(Err(e)) => Err(e),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Connection closed by server unexpectedly",
            )),
        }
    }

    /// Initiates a real-time tail subscription query on a specific stream.
    pub async fn tail_stream(mut self, stream_name: &str, format: &str) -> io::Result<()> {
        let query_str = format!("tail(\"{}\")", stream_name);
        self.framed.send(LivenFrame::Query(query_str)).await?;

        while let Some(res) = self.framed.next().await {
            match res? {
                LivenFrame::Records(records) => {
                    for record in records {
                        if format == "json" {
                            match serde_json::to_string(&record) {
                                Ok(json_str) => println!("{}", json_str),
                                Err(e) => eprintln!("Failed to serialize record to JSON: {}", e),
                            }
                        } else {
                            let val_str = match &record.value {
                                crate::types::DataValue::Null => "NULL".to_string(),
                                crate::types::DataValue::Bool(b) => b.to_string(),
                                crate::types::DataValue::Int(i) => i.to_string(),
                                crate::types::DataValue::UInt(u) => u.to_string(),
                                crate::types::DataValue::Float(f) => f.to_string(),
                                crate::types::DataValue::String(s) => s.clone(),
                                crate::types::DataValue::Binary(b) => {
                                    format!("<Binary: {} bytes>", b.len())
                                }
                                crate::types::DataValue::Array(arr) => format!("{:?}", arr),
                                crate::types::DataValue::Object(obj) => format!("{:?}", obj),
                                crate::types::DataValue::Vector(vec) => format!("{:?}", vec),
                            };
                            println!(
                                "\x1b[32m[tail]\x1b[0m \x1b[1mSeq:\x1b[0m #{} | \x1b[1mKey:\x1b[0m {} | \x1b[1mValue:\x1b[0m {}",
                                record.sequence_id, record.key, val_str
                            );
                        }
                    }
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unexpected response frame during tail: {:?}", other),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Initiates a real-time query subscription on a specific stream using the tail method,
    /// returning an asynchronous Stream of Record items.
    pub async fn listen(
        mut self,
        stream_name: &str,
    ) -> io::Result<
        std::pin::Pin<Box<dyn futures_util::Stream<Item = io::Result<Record>> + Send + 'static>>,
    > {
        let query_str = format!("tail(\"{}\")", stream_name);
        self.framed.send(LivenFrame::Query(query_str)).await?;

        let state = (self.framed, Vec::<Record>::new());
        let stream = futures_util::stream::unfold(state, |(mut framed, mut buffer)| async move {
            loop {
                if !buffer.is_empty() {
                    let rec = buffer.remove(0);
                    return Some((Ok(rec), (framed, buffer)));
                }

                match framed.next().await {
                    Some(Ok(LivenFrame::Records(mut records))) => {
                        if !records.is_empty() {
                            let rec = records.remove(0);
                            return Some((Ok(rec), (framed, records)));
                        }
                    }
                    Some(Ok(other)) => {
                        return Some((
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Unexpected frame during listen: {:?}", other),
                            )),
                            (framed, Vec::new()),
                        ));
                    }
                    Some(Err(e)) => {
                        return Some((Err(e), (framed, Vec::new())));
                    }
                    None => return None,
                }
            }
        });

        Ok(Box::pin(stream))
    }
}
