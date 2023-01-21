use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::{Context, Poll};
use bytes::BytesMut;
use tokio_tower::Error;
use tokio_tower::multiplex::{Client, MultiplexTransport, TagStore};
use tokio_tower::multiplex::client::VecDequePendingStore;
use tokio_util::codec::Framed;
use tower::Service;
use crate::client::codec::KafkaClientCodec;

pub mod codec;


pub struct KafkaClient;

impl <T> Service<T> for KafkaClient
    where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static
{
    type Response = TokioTowerClient<T>;
    type Error = TokioTowerError<T>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, io: T) -> Self::Future {
            Box::pin(async move {
                let client_codec = KafkaClientCodec::new();
                let tx = Framed::new(io, client_codec);
                Ok(Client::builder(MultiplexTransport::new(tx, CorrelationStore::default()))
                    .pending_store(VecDequePendingStore::default())
                    .build())
            })
    }
}
