use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;

pub trait MakeConnection {
    type Connection: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sized;
    type Error: Error;
    type Future: Future<Output=Result<Self::Connection, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    fn connect(self) -> Self::Future;
}

struct TcpConnection {
    addr: SocketAddr,
}

impl TcpConnection {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr
        }
    }
}

impl MakeConnection for TcpConnection {
    type Connection = TcpStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output=io::Result<Self::Connection>> + Send + Sync + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn connect(self) -> Self::Future {
        Box::pin(async move {
            let tcp_stream = TcpStream::connect(self.addr).await?;
            Ok(tcp_stream)
        })
    }
}