use crate::transport::KafkaTransportError;
use kafka_protocol::protocol::{DecodeError, EncodeError};
use std::fmt::{Display, Formatter};

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
        KafkaError::Serde
    }
}

impl From<EncodeError> for KafkaError {
    fn from(_: EncodeError) -> Self {
        KafkaError::Serde
    }
}

impl From<KafkaTransportError> for KafkaError {
    fn from(value: KafkaTransportError) -> Self {
        KafkaError::Transport(value)
    }
}
