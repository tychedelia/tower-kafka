use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI32, Ordering};

use bytes::{BytesMut, Bytes, Buf};
use futures::{Sink, TryStream};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, MetadataRequest, MetadataResponse, RequestHeader, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::protocol::{Decodable, DecodeError, Encodable, EncodeError, HeaderVersion, Request};
use kafka_protocol::protocol::buf::ByteBuf;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio_tower::Error;
use tokio_tower::multiplex::{
    Client, client::VecDequePendingStore, MultiplexTransport, Server, TagStore,
};
use tokio_util::codec;
use tokio_util::codec::{Framed, FramedParts, FramedWrite};
use tower::Service;

#[derive(Debug)]
pub struct KafkaClientCodec {
    length_codec: codec::LengthDelimitedCodec,
}

impl KafkaClientCodec {
    pub fn new() -> Self {
        Self {
            length_codec: codec::LengthDelimitedCodec::builder()
                .max_frame_length(i32::MAX as usize)
                .length_field_length(4)
                .new_codec(),
        }
    }
}

impl codec::Encoder<BytesMut> for KafkaClientCodec {
    type Error = io::Error;

    fn encode(
        &mut self,
        mut item: BytesMut,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.length_codec.encode(item.get_bytes(item.len()), dst)?;
        Ok(())
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