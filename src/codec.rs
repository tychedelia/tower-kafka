
use std::fmt::{Debug};

use std::io;
use bytes::{BytesMut};
use kafka_protocol::protocol::buf::ByteBuf;
use tokio_util::codec;

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