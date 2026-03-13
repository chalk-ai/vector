use std::net::SocketAddr;

use vector_lib::event::{Event, LogEvent};

use super::TestListener;
use crate::test_util::CountReceiver;

/// Captures newline-delimited data from a socket sink.
///
/// Wraps [`CountReceiver::receive_lines()`] which binds the port in `start()` and
/// collects all lines until shutdown.
pub struct TcpListener {
    pub addr: SocketAddr,
    receiver: Option<CountReceiver<String>>,
}

impl TcpListener {
    pub const fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            receiver: None,
        }
    }
}

#[async_trait::async_trait]
impl TestListener for TcpListener {
    async fn start(&mut self) -> Result<(), String> {
        self.receiver = Some(CountReceiver::receive_lines(self.addr));
        // CountReceiver::receive_lines binds the port synchronously in a tokio::spawn,
        // so wait until it's actually accepting connections.
        crate::test_util::wait_for_tcp(self.addr).await;
        Ok(())
    }

    async fn collect(&mut self) -> Vec<Event> {
        match self.receiver.take() {
            Some(receiver) => receiver
                .await
                .into_iter()
                .map(|line| Event::Log(LogEvent::from_str_legacy(line)))
                .collect(),
            None => Vec::new(),
        }
    }
}
