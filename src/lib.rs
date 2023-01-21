use std::error::Error;
/// # tower-kafka
///
/// A tower service for interacting with Apache Kafka.
///
/// ## Example
///
/// ```rust
/// use tower_kafka::KafkaSvc;
///
/// #[tokio::main]
/// async fn main() -> std::io::Result<()> {
///     use tokio::net::TcpStream;
///     let tcp_stream = TcpStream::connect("127.0.0.1:9093".parse().unwrap()).await?;
///     let svc = KafkaSvc::new(tcp_stream);
/// }
/// ```
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::process::Output;
use std::task::{Context, Poll};
use bytes::{Bytes, BytesMut};
use futures::future::Future;
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, DecodeError, Encodable, EncodeError, HeaderVersion, Message, Request};
use tower::Service;
use crate::connect::MakeConnection;
use crate::KafkaError::Serde;
use crate::transport::{KafkaTransportError, KafkaTransportSvc};

pub mod transport;
pub mod connect;

pub struct KafkaSvc<T> {
    inner: KafkaTransportSvc<T>,
}

// impl <C> KafkaSvc<C::Connection>
//     where C: MakeConnection
// {
//     pub async fn new(connection: C) -> Result<Self, C::Error> {
//         let inner = KafkaTransportSvc::new(connection).await?;
//         Ok(Self { inner })
//     }
// }

#[derive(thiserror::Error, Debug)]
pub enum KafkaError {
    Transport(KafkaTransportError),
    Serde,
}

impl Display for KafkaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<DecodeError> for KafkaError {
    fn from(_: DecodeError) -> Self {
        Serde
    }
}

impl From<EncodeError> for KafkaError {
    fn from(_: EncodeError) -> Self {
        Serde
    }
}

pub type KafkaRequest<Req> = (RequestHeader, Req);
pub type KafkaResponse<Res> = (ResponseHeader, Res);

impl<Req, S, E> Service<KafkaRequest<Req>> for KafkaSvc<S>
    where Req: Request + Message + Encodable + HeaderVersion,
          S: Service<BytesMut, Response=BytesMut, Error=E>,
          <S as Service<BytesMut>>::Error: Debug,
          <S as Service<BytesMut>>::Future: 'static,
          E: Error + Into<KafkaError>, KafkaError: From<E>
{
    type Response = KafkaResponse<Req::Response>;
    type Error = KafkaError;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: KafkaRequest<Req>) -> Self::Future {
        Box::pin(async move {
            let version = req.0.request_api_version;
            let mut bytes = BytesMut::new();
            req.0.encode(&mut bytes, <Req as HeaderVersion>::header_version(version))?;
            req.1.encode(&mut bytes, version)?;

            let mut res_bytes = self.inner.call(bytes).await?;

            let header = ResponseHeader::decode(&mut res_bytes, <Req::Response as HeaderVersion>::header_version(version))?;
            let response = <Req::Response as Decodable>::decode(&mut res_bytes, version)?;
            Ok((header, response))
        })
    }
}