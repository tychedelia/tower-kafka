use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::BytesMut;
use futures::future::Future;
use futures::{TryFutureExt};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Message, Request};
use crate::client::{KafkaTransportError};

mod codec;
pub mod client;


pub struct TowerKafka<C> {
    client: C,
}

impl <C> TowerKafka<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }
}


pub type KafkaRequest<Req> = (RequestHeader, Req);

type KafkaResponse<Res> = (ResponseHeader, Res);

impl<Req, C> tower::Service<KafkaRequest<Req>> for TowerKafka<C>
    where Req: Request + Message + Encodable + HeaderVersion,
          C: tower::Service<BytesMut, Response=BytesMut>,
          <C as tower::Service<BytesMut>>::Error: Debug,
          <C as tower::Service<BytesMut>>::Future: 'static
{
    type Response = KafkaResponse<Req::Response>;
    type Error = KafkaTransportError;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: KafkaRequest<Req>) -> Self::Future {
        let version = req.0.request_api_version;
        let mut bytes = BytesMut::new();

        req.0.encode(&mut bytes, <Req as HeaderVersion>::header_version(version)).unwrap();
        req.1.encode(&mut bytes, version).unwrap();

        let fut = self.client.call(bytes)
            .map_err(|e| {
                    println!("{:?}", e);
                    KafkaTransportError::ClientDropped
                });

        Box::pin(async move {
            let mut res_bytes = fut.await?;
            let header = ResponseHeader::decode(&mut res_bytes, <Req::Response as HeaderVersion>::header_version(version)).unwrap();
            let response = <Req::Response as Decodable>::decode(&mut res_bytes, version).unwrap();
            Ok((header, response ))
        })
    }
}