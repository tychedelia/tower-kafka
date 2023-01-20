use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::BytesMut;
use futures::future::Future;
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Message, Request};
use crate::transport::{KafkaTransportError, KafkaTransportSvc, TokioTowerClient};

mod codec;
pub mod transport;


pub struct KafkaSvc<T> {
    inner: T,
}

impl <IO> KafkaSvc<KafkaTransportSvc<TokioTowerClient<IO>>>
    where IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static
{
    pub fn new(io: IO) -> Self
    {
        let inner = KafkaTransportSvc::new(io);
        Self { inner }
    }
}


pub type KafkaRequest<Req> = (RequestHeader, Req);

type KafkaResponse<Res> = (ResponseHeader, Res);

impl<Req, T> tower::Service<KafkaRequest<Req>> for KafkaSvc<T>
    where Req: Request + Message + Encodable + HeaderVersion,
          T: tower::Service<BytesMut, Response=BytesMut, Error=KafkaTransportError>,
          <T as tower::Service<BytesMut>>::Error: Debug,
          <T as tower::Service<BytesMut>>::Future: 'static
{
    type Response = KafkaResponse<Req::Response>;
    type Error = KafkaTransportError;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: KafkaRequest<Req>) -> Self::Future {
        let version = req.0.request_api_version;
        let mut bytes = BytesMut::new();

        req.0.encode(&mut bytes, <Req as HeaderVersion>::header_version(version)).unwrap();
        req.1.encode(&mut bytes, version).unwrap();

        let fut = self.inner.call(bytes);
        Box::pin(async move {
            let mut res_bytes = fut.await?;
            let header = ResponseHeader::decode(&mut res_bytes, <Req::Response as HeaderVersion>::header_version(version)).unwrap();
            let response = <Req::Response as Decodable>::decode(&mut res_bytes, version).unwrap();
            Ok((header, response ))
        })
    }
}