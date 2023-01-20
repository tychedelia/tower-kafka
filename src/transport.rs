use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::TryFutureExt;
use tokio_tower::Error;
use tokio_tower::multiplex::{
    Client, client::VecDequePendingStore, MultiplexTransport, TagStore,
};
use tokio_util::codec::Framed;
use tower::Service;

use crate::codec::KafkaClientCodec;

// `tokio-tower` tag store for the Kafka protocol.
#[derive(Default)]
pub struct CorrelationStore {
    correlation_ids: HashSet<i32>,
    id_gen: AtomicI32,
}

const REQUEST_CORRELATION_ID_OFFSET: usize = 8;
const RESPONSE_CORRELATION_ID_OFFSET: usize = 0;

impl TagStore<BytesMut, BytesMut> for CorrelationStore {
    type Tag = i32;

    fn assign_tag(self: Pin<&mut Self>, request: &mut BytesMut) -> i32 {
        let tag = self.id_gen.fetch_add(1, Ordering::SeqCst);
        request[REQUEST_CORRELATION_ID_OFFSET..REQUEST_CORRELATION_ID_OFFSET+4].copy_from_slice(&tag.to_be_bytes());
        tag
    }

    fn finish_tag(mut self: Pin<&mut Self>, response: &BytesMut) -> i32 {
        let tag = i32::from_be_bytes(response[RESPONSE_CORRELATION_ID_OFFSET..RESPONSE_CORRELATION_ID_OFFSET+4].try_into().unwrap());
        self.correlation_ids.remove(&tag);
        tag
    }
}

#[derive(thiserror::Error, Debug)]
pub enum KafkaTransportError {
    BrokenTransportSend,
    BrokenTransportRecv,
    Cancelled,
    TransportFull,
    ClientDropped,
    Desynchronized,
    TransportDropped,
    Unknown,
}

impl Display for KafkaTransportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl <T> From<TokioTowerError<T>> for KafkaTransportError
    where T: tokio::io::AsyncWrite + tokio::io::AsyncRead
{
    fn from(value: TokioTowerError<T>) -> Self {
        match value {
            TokioTowerError::BrokenTransportSend(_) => KafkaTransportError::BrokenTransportSend,
            TokioTowerError::BrokenTransportRecv(_) => KafkaTransportError::BrokenTransportRecv,
            TokioTowerError::Cancelled => KafkaTransportError::Cancelled,
            TokioTowerError::TransportFull => KafkaTransportError::TransportFull,
            TokioTowerError::ClientDropped => KafkaTransportError::ClientDropped,
            TokioTowerError::Desynchronized => KafkaTransportError::Desynchronized,
            TokioTowerError::TransportDropped => KafkaTransportError::TransportDropped,
            _ => KafkaTransportError::Unknown,
        }
    }
}

type FramedIO<T> = Framed<T, KafkaClientCodec>;

type TokioTowerError<T> = Error<
    MultiplexTransport<FramedIO<T>, CorrelationStore>, BytesMut
>;

pub type TokioTowerClient<T> = Client<
    MultiplexTransport<FramedIO<T>, CorrelationStore>,
    TokioTowerError<T>,
    BytesMut
>;

pub struct KafkaTransportSvc<C> {
    client: C
}

impl <IO> KafkaTransportSvc<TokioTowerClient<IO>>
    where IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static
{

    pub fn new(io: IO) -> Self {
        let client_codec = KafkaClientCodec::new();
        let tx = Framed::new(io, client_codec);

        let client = Client::builder(MultiplexTransport::new(tx, CorrelationStore::default()))
            .pending_store(VecDequePendingStore::default())
            .build();

        Self {
            client
        }
    }
}

impl <C, IO> Service<BytesMut> for KafkaTransportSvc<C>
    where C: Service<BytesMut, Error=TokioTowerError<IO>> + 'static,
          IO: tokio::io::AsyncRead + tokio::io::AsyncWrite
{
    type Response = C::Response;
    type Error = KafkaTransportError;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.client.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: BytesMut) -> Self::Future {
        Box::pin(self.client.call(req).map_err(|e| e.into()))
    }
}