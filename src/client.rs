use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::process::Output;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::{Context, Poll};

use bytes::{BytesMut, Bytes, Buf};
use futures::{AsyncRead, AsyncWrite, Sink, TryFutureExt, TryStream};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, MetadataRequest, MetadataResponse, RequestHeader, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::protocol::{Decodable, DecodeError, Encodable, EncodeError, HeaderVersion};
use kafka_protocol::protocol::buf::ByteBuf;
use tokio::net::TcpStream;
use tokio_tower::{Error, multiplex};
use tokio_tower::multiplex::{
    Client, client::VecDequePendingStore, MultiplexTransport, Server, TagStore,
};
use tokio_util::codec;
use tokio_util::codec::{Decoder, Encoder, Framed, FramedParts, FramedWrite};
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

    fn assign_tag(mut self: Pin<&mut Self>, request: &mut BytesMut) -> i32 {
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

pub struct KafkaClient {
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

impl <T> From<KafkaClientSvcError<T>> for KafkaTransportError
    where T: tokio::io::AsyncWrite + tokio::io::AsyncRead
{
    fn from(value: KafkaClientSvcError<T>) -> Self {
        match value {
            KafkaClientSvcError::BrokenTransportSend(_) => KafkaTransportError::BrokenTransportSend,
            KafkaClientSvcError::BrokenTransportRecv(_) => KafkaTransportError::BrokenTransportRecv,
            KafkaClientSvcError::Cancelled => KafkaTransportError::Cancelled,
            KafkaClientSvcError::TransportFull => KafkaTransportError::TransportFull,
            KafkaClientSvcError::ClientDropped => KafkaTransportError::ClientDropped,
            KafkaClientSvcError::Desynchronized => KafkaTransportError::Desynchronized,
            KafkaClientSvcError::TransportDropped => KafkaTransportError::TransportDropped,
            _ => KafkaTransportError::Unknown,
        }
    }
}

struct Request {
    header: RequestHeader,
    body: BytesMut
}

struct Response {
    header: ResponseHeader,
    body: BytesMut
}

type WrappedTransport<T> = Framed<T, KafkaClientCodec>;

type KafkaClientSvcError<T> = Error<
    MultiplexTransport<WrappedTransport<T>, CorrelationStore>, BytesMut
>;

type KafkaClientSvc<T> = Client<
    MultiplexTransport<WrappedTransport<T>, CorrelationStore>,
    KafkaClientSvcError<T>,
    BytesMut
>;

async fn ready<S: Service<Request>, Request>(svc: &mut S) -> Result<(), S::Error> {
    poll_fn(|cx| svc.poll_ready(cx)).await
}


impl KafkaClient {
    pub async fn connect<T>(tx: T) -> Result<KafkaClientSvc<T>, KafkaClientSvcError<T>>
        where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static
    {
        let client_codec = KafkaClientCodec::new();
        let tx = Framed::new(tx, client_codec);

        let mut client = Client::builder(MultiplexTransport::new(tx, CorrelationStore::default()))
            .pending_store(VecDequePendingStore::default())
            .build();

        ready(&mut client).await?;
        Ok(client)
    }
}

pub struct KafkaTransportSvc<C> {
    client: C
}

impl <C, T> Service<BytesMut> for KafkaTransportSvc<C>
    where C: Service<BytesMut, Error=KafkaClientSvcError<T>> + 'static,
          T: tokio::io::AsyncRead + tokio::io::AsyncWrite
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