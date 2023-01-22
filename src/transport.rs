use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::{Context, Poll};

use crate::connect::MakeConnection;
use bytes::BytesMut;
use futures::TryFutureExt;
use kafka_protocol::protocol::buf::ByteBuf;
use tokio_tower::multiplex::{client::VecDequePendingStore, Client, MultiplexTransport, TagStore};
use tokio_tower::Error;
use tokio_util::codec;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tower::Service;

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
        request[REQUEST_CORRELATION_ID_OFFSET..REQUEST_CORRELATION_ID_OFFSET + 4]
            .copy_from_slice(&tag.to_be_bytes());
        tag
    }

    fn finish_tag(mut self: Pin<&mut Self>, response: &BytesMut) -> i32 {
        let tag = i32::from_be_bytes(
            response[RESPONSE_CORRELATION_ID_OFFSET..RESPONSE_CORRELATION_ID_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        self.correlation_ids.remove(&tag);
        tag
    }
}

type FramedIO<T> = Framed<T, KafkaClientCodec>;
pub type TransportError<T> = Error<MultiplexTransport<FramedIO<T>, CorrelationStore>, BytesMut>;
pub type TransportClient<T> =
    Client<MultiplexTransport<FramedIO<T>, CorrelationStore>, TransportError<T>, BytesMut>;

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

impl<T> From<TransportError<T>> for KafkaTransportError
where
    T: tokio::io::AsyncWrite + tokio::io::AsyncRead,
{
    fn from(value: TransportError<T>) -> Self {
        match value {
            TransportError::BrokenTransportSend(_) => KafkaTransportError::BrokenTransportSend,
            TransportError::BrokenTransportRecv(_) => KafkaTransportError::BrokenTransportRecv,
            TransportError::Cancelled => KafkaTransportError::Cancelled,
            TransportError::TransportFull => KafkaTransportError::TransportFull,
            TransportError::ClientDropped => KafkaTransportError::ClientDropped,
            TransportError::Desynchronized => KafkaTransportError::Desynchronized,
            TransportError::TransportDropped => KafkaTransportError::TransportDropped,
            _ => KafkaTransportError::Unknown,
        }
    }
}

/// A simple wrapper around [`codec::LengthDelimitedCodec`], which ensures
/// protocol frames are well formed.
#[derive(Debug)]
pub struct KafkaClientCodec {
    length_codec: LengthDelimitedCodec,
}

impl KafkaClientCodec {
    /// Create a new codec.
    pub fn new() -> Self {
        Self {
            length_codec: LengthDelimitedCodec::builder()
                .max_frame_length(i32::MAX as usize)
                .length_field_length(4)
                .new_codec(),
        }
    }
}

impl codec::Encoder<BytesMut> for KafkaClientCodec {
    type Error = io::Error;

    fn encode(&mut self, mut item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.length_codec.encode(item.get_bytes(item.len()), dst)?;
        Ok(())
    }
}

pub struct MakeClient<C> {
    connection: C,
}

impl<C> MakeClient<C>
where
    C: MakeConnection + 'static,
{
    pub fn with_connection(connection: C) -> Self {
        Self { connection }
    }

    pub async fn into_client(self) -> Result<TransportClient<C::Connection>, C::Error> {
        let io = self.connection.connect().await?;
        let io = Framed::new(io, KafkaClientCodec::new());

        let client = Client::builder(MultiplexTransport::new(io, CorrelationStore::default()))
            .pending_store(VecDequePendingStore::default())
            .build();

        Ok(client)
    }
}

impl codec::Decoder for KafkaClientCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(bytes) = self.length_codec.decode(src)? {
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }
}

pub struct KafkaTransportService<Svc> {
    inner: Svc,
}

impl<Svc> KafkaTransportService<Svc> {
    pub fn new(inner: Svc) -> Self {
        Self { inner }
    }
}

impl<Svc> Service<BytesMut> for KafkaTransportService<Svc>
where
    Svc: Service<BytesMut, Response = BytesMut> + 'static,
    Svc::Error: Into<KafkaTransportError>,
{
    type Response = Svc::Response;
    type Error = KafkaTransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: BytesMut) -> Self::Future {
        Box::pin(self.inner.call(req).map_err(|e| e.into()))
    }
}
