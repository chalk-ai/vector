use std::{io::Read, net::SocketAddr, time::Duration};

use bytes::Bytes;
use flate2::read::MultiGzDecoder;
use futures::{FutureExt, SinkExt, TryFutureExt, channel::mpsc};
use futures_util::StreamExt;
use http::request::Parts;
use hyper::{
    Body, Request, Response, Server, StatusCode,
    service::{make_service_fn, service_fn},
};
use stream_cancel::{Trigger, Tripwire};
use vector_lib::event::{Event, LogEvent};

use super::TestListener;
use crate::{Error, test_util::wait_for_tcp};

/// How captured HTTP bodies should be decompressed before decoding.
#[derive(Debug, Clone, Default)]
pub enum Decompression {
    #[default]
    None,
    Gzip,
}

/// How captured HTTP bodies should be decoded into events.
#[derive(Debug, Clone)]
pub enum BodyDecoding {
    /// Parse bodies as JSON. Handles both arrays (`[{...},{...}]`) and
    /// newline-delimited JSON (`{...}\n{...}`).
    Json,
}

/// An in-process HTTP server that captures all request bodies for assertion.
///
/// Unlike `build_test_server_generic()`, this server captures bodies on **every**
/// request — including non-2xx responses — so retry-count assertions work correctly.
pub struct HttpListener {
    pub addr: SocketAddr,
    pub status_code: StatusCode,
    pub decompression: Decompression,
    pub decoding: BodyDecoding,
    rx: Option<mpsc::Receiver<(Parts, Bytes)>>,
    trigger: Option<Trigger>,
}

impl HttpListener {
    pub fn new(
        addr: SocketAddr,
        status_code: u16,
        decompression: Decompression,
        decoding: BodyDecoding,
    ) -> Self {
        Self {
            addr,
            status_code: StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK),
            decompression,
            decoding,
            rx: None,
            trigger: None,
        }
    }
}

#[async_trait::async_trait]
impl TestListener for HttpListener {
    async fn start(&mut self) -> Result<(), String> {
        let (tx, rx) = mpsc::channel(256);
        let (trigger, tripwire) = Tripwire::new();
        let status = self.status_code;
        let addr = self.addr;

        let service = make_service_fn(move |_| {
            let tx = tx.clone();
            async move {
                Ok::<_, Error>(service_fn(move |req: Request<Body>| {
                    let mut tx = tx.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        // Capture the body before responding — always, regardless of status code.
                        let bytes = http_body::Body::collect(body)
                            .await
                            .map(|b| b.to_bytes())
                            .unwrap_or_default();
                        tokio::spawn(async move {
                            let _ = tx.send((parts, bytes)).await;
                        });
                        Ok::<_, Error>(
                            Response::builder()
                                .status(status)
                                .body(Body::empty())
                                .unwrap(),
                        )
                    }
                }))
            }
        });

        let server = Server::try_bind(&addr)
            .map_err(|e| format!("HttpListener failed to bind {addr}: {e}"))?
            .serve(service)
            .with_graceful_shutdown(tripwire.then(crate::shutdown::tripwire_handler))
            .map_err(|error| panic!("HttpListener server error: {error}"));

        tokio::spawn(server);

        self.rx = Some(rx);
        self.trigger = Some(trigger);

        // Wait until the server is accepting connections before returning.
        tokio::time::timeout(Duration::from_secs(5), wait_for_tcp(addr))
            .await
            .map_err(|_| format!("HttpListener on {addr} did not bind within 5s"))
    }

    async fn collect(&mut self) -> Vec<Event> {
        // Signal shutdown so the server stops accepting new requests.
        drop(self.trigger.take());

        let rx = match self.rx.take() {
            Some(rx) => rx,
            None => return Vec::new(),
        };

        // Drain the channel — all bodies that were sent before shutdown.
        let bodies: Vec<Bytes> = rx
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|(_, b)| b)
            .collect();

        let mut events = Vec::new();
        for body in bodies {
            let raw = match decompress(&self.decompression, body) {
                Ok(b) => b,
                Err(e) => {
                    warn!("HttpListener decompression error: {e}");
                    continue;
                }
            };
            events.extend(decode_body(&self.decoding, raw));
        }
        events
    }
}

fn decompress(mode: &Decompression, bytes: Bytes) -> Result<Vec<u8>, String> {
    match mode {
        Decompression::None => Ok(bytes.to_vec()),
        Decompression::Gzip => {
            let mut decoder = MultiGzDecoder::new(&bytes[..]);
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("gzip decompression failed: {e}"))?;
            Ok(out)
        }
    }
}

fn decode_body(mode: &BodyDecoding, raw: Vec<u8>) -> Vec<Event> {
    match mode {
        BodyDecoding::Json => decode_json_body(&raw),
    }
}

/// Parses a JSON body into events.
///
/// Handles three shapes:
/// - A JSON array: `[{...}, {...}]` — each element becomes one event.
/// - A single JSON object: `{...}` — becomes one event.
/// - Newline-delimited JSON: one JSON object per line.
fn decode_json_body(raw: &[u8]) -> Vec<Event> {
    let s = match std::str::from_utf8(raw) {
        Ok(s) => s.trim(),
        Err(_) => return Vec::new(),
    };

    // Try JSON array first.
    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(s) {
        return arr
            .into_iter()
            .filter_map(|v| {
                v.as_object().map(|obj| {
                    let mut log = LogEvent::default();
                    for (k, v) in obj {
                        if let Ok(val) = serde_json::from_value::<vrl::value::Value>(v.clone()) {
                            log.insert(k.as_str(), val);
                        }
                    }
                    Event::Log(log)
                })
            })
            .collect();
    }

    // Try single JSON object.
    if let Ok(obj) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(s) {
        let mut log = LogEvent::default();
        for (k, v) in obj {
            if let Ok(val) = serde_json::from_value::<vrl::value::Value>(v) {
                log.insert(k.as_str(), val);
            }
        }
        return vec![Event::Log(log)];
    }

    // Try newline-delimited JSON.
    s.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(line)
                .ok()
                .map(|obj| {
                    let mut log = LogEvent::default();
                    for (k, v) in obj {
                        if let Ok(val) = serde_json::from_value::<vrl::value::Value>(v) {
                            log.insert(k.as_str(), val);
                        }
                    }
                    Event::Log(log)
                })
        })
        .collect()
}
