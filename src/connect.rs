//! Utilities for defining an underlying connection for the Kafka transport. Currently, we only
//! expose a basic TCP connection, but in the future this can handle functionality like SASL, SSL,
//! etc., without leaking these details into our main service.
use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;

/// Helper for defining a "connection" that provides [`AsyncRead`] and [`AsyncWrite`] for
/// sending messages to Kafka.
pub trait MakeConnection {
    /// The connection type.
    type Connection: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sized;
    /// The error that may be produced when connecting.
    type Error: Error;
    /// The future used for awaiting a connection.
    type Future: Future<Output = Result<Self::Connection, Self::Error>>;

    /// Check whether a connection is ready to be produced.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Connect to the connection.
    fn connect(self) -> Self::Future;
}

/// A simple TCP connection.
pub struct TcpConnection {
    addr: SocketAddr,
}

impl TcpConnection {
    /// Create a new connection using the provided socket address.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

impl MakeConnection for TcpConnection {
    type Connection = TcpStream;
    type Error = io::Error;
    type Future =
        Pin<Box<dyn Future<Output = io::Result<Self::Connection>> + Send + Sync + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn connect(self) -> Self::Future {
        Box::pin(async move {
            let tcp_stream = TcpStream::connect(self.addr).await?;
            Ok(tcp_stream)
        })
    }
}
