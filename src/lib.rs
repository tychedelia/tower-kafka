//! # tower-kafka
//!
//! A tower service for interacting with Apache Kafka.
//!
//! ## Example
//!
//! ```no_run
//! use tower_kafka::KafkaService;
//!
//! # #[tokio::main]
//! # async fn main() -> std::io::Result<()> {
//!     use tower_kafka::connect::TcpConnection;
//!     use tower_kafka::MakeService;
//!     let connection = TcpConnection::new("127.0.0.1:9093".parse().unwrap());
//!     let svc = MakeService::new(connection).into_service().await.unwrap();
//! #   Ok(())
//! # }
//! ```
#![deny(missing_docs)]

use crate::connect::MakeConnection;
use crate::error::KafkaError;
use crate::transport::{KafkaTransportService, MakeClient, TransportClient};
use bytes::BytesMut;
use futures::future::Future;
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Message, Request};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

pub mod connect;
pub mod error;
pub mod transport;

/// A service for interacting with Apache Kafka.
pub struct KafkaService<Svc> {
    inner: Svc,
}

impl<Svc> KafkaService<Svc> {
    /// Create a new service wrapping the provided transport service.
    pub fn new(inner: Svc) -> Self {
        Self { inner }
    }

    fn encode<Req>(req: KafkaRequest<Req>) -> Result<BytesMut, KafkaError>
    where
        Req: Message + HeaderVersion + Encodable,
    {
        let version = req.0.request_api_version;
        let mut bytes = BytesMut::new();
        req.0
            .encode(&mut bytes, <Req as HeaderVersion>::header_version(version))?;
        req.1.encode(&mut bytes, version)?;
        Ok(bytes)
    }

    fn decode<Res>(mut bytes: BytesMut, version: i16) -> Result<KafkaResponse<Res>, KafkaError>
    where
        Res: Message + HeaderVersion + Decodable,
    {
        let header =
            ResponseHeader::decode(&mut bytes, <Res as HeaderVersion>::header_version(version))?;
        let response = <Res as Decodable>::decode(&mut bytes, version)?;
        Ok((header, response))
    }
}

/// Helper for constructing new service instances from a connection.
pub struct MakeService<C> {
    connection: C,
}

impl<C> MakeService<C>
where
    C: MakeConnection + 'static,
{
    /// Create a new helper with the provided connection.
    pub fn new(connection: C) -> Self {
        Self { connection }
    }

    /// Wait for connection and a new service instance.
    pub async fn into_service(
        self,
    ) -> Result<KafkaService<KafkaTransportService<TransportClient<C::Connection>>>, C::Error> {
        let client = MakeClient::with_connection(self.connection)
            .into_client()
            .await?;
        let transport = KafkaTransportService::new(client);
        Ok(KafkaService::new(transport))
    }
}

/// A Kafka request.
pub type KafkaRequest<Req> = (RequestHeader, Req);
/// A Kafka response.
pub type KafkaResponse<Res> = (ResponseHeader, Res);

impl<Req, Svc> Service<KafkaRequest<Req>> for KafkaService<Svc>
where
    Req: Request + Message + Encodable + HeaderVersion,
    Svc: Service<BytesMut, Response = BytesMut>,
    <Svc as Service<BytesMut>>::Error: Into<KafkaError>,
    <Svc as Service<BytesMut>>::Future: 'static,
{
    type Response = KafkaResponse<Req::Response>;
    type Error = KafkaError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: KafkaRequest<Req>) -> Self::Future {
        let version = req.0.request_api_version;
        let encoded = Self::encode(req).unwrap();
        let fut = self.inner.call(encoded);
        Box::pin(async move {
            let res_bytes = fut.await.map_err(|e| e.into())?;
            let response = Self::decode(res_bytes, version)?;
            Ok(response)
        })
    }
}
