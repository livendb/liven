use crate::codec::{KondaCodec, KondaFrame};
use crate::types::Record;
use futures_util::{SinkExt, StreamExt};
use std::io;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub struct KondaClient {
    framed: Framed<TcpStream, KondaCodec>,
}

impl KondaClient {
    /// Connects to a Konda server instance over the native wire protocol.
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let client_id = "default_client".to_string();
        Self::connect_with_id(addr, &client_id).await
    }

    /// Connects to a Konda server instance with a specific client ID for auth.
    pub async fn connect_with_id(addr: &str, client_id: &str) -> io::Result<Self> {
        let mode = if let Ok(config) = crate::config::AppConfig::load() {
            config.security.mode.clone()
        } else {
            "none".to_string()
        };
        Self::connect_with_auth_mode(addr, client_id, &mode).await
    }

    /// Connects to a Konda server instance with an explicit client ID and security mode.
    pub async fn connect_with_auth_mode(
        addr: &str,
        client_id: &str,
        mode: &str,
    ) -> io::Result<Self> {
        let stripped_scheme = if addr.starts_with("konda://") {
            &addr["konda://".len()..]
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

        // Wrap with our client-configured KondaCodec (is_client: true)
        let mut framed = Framed::new(tcp_stream, KondaCodec::new(true));

        let do_auth = mode == "auth_key" || parsed_auth_key.is_some();

        if do_auth {
            let token_to_send = if let Some(key) = parsed_auth_key {
                key
            } else {
                client_id.to_string()
            };

            // 1. Send Connect frame containing the symmetric token
            framed
                .send(KondaFrame::Connect {
                    client_id: token_to_send,
                })
                .await?;

            // 2. Expect Ok or Err frame
            match framed.next().await {
                Some(Ok(KondaFrame::Ok)) => {}
                Some(Ok(KondaFrame::Err(e))) => {
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

    pub fn into_inner(self) -> Framed<TcpStream, KondaCodec> {
        self.framed
    }

    /// Submits a query expression over the wire and awaits deserialized Records response.
    pub async fn query(&mut self, query_str: &str) -> io::Result<Vec<Record>> {
        self.framed
            .send(KondaFrame::Query(query_str.to_string()))
            .await?;

        match self.framed.next().await {
            Some(Ok(KondaFrame::Records(records))) => Ok(records),
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
        self.framed.send(KondaFrame::Query(query_str)).await?;

        while let Some(res) = self.framed.next().await {
            match res? {
                KondaFrame::Records(records) => {
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
}
