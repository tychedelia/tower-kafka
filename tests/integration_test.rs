use kafka_protocol::messages::{ApiKey, MetadataRequest, RequestHeader};
use kafka_protocol::protocol::StrBytes;
use tokio::net::TcpStream;
use tower::ServiceExt;
use tower_kafka::transport::{KafkaTransportError};
use tower_kafka::{KafkaSvc};

// Make sure to run kafka from docker-compose in root of project.
#[tokio::test]
async fn test() -> Result<(), KafkaTransportError> {
    let stream = TcpStream::connect("127.0.0.1:9092").await.unwrap();
    let svc = KafkaSvc::new(stream);
    let mut header = RequestHeader::default();
    header.client_id = Some(StrBytes::from_str("hi"));
    header.request_api_key = ApiKey::MetadataKey as i16;
    header.request_api_version = 12;
    let mut request = MetadataRequest::default();
    request.topics = None;
    request.allow_auto_topic_creation = true;

    svc.oneshot((
        header,
        request,
    )).await?;


    Ok(())
}