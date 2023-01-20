use kafka_protocol::messages::{ApiKey, MetadataRequest, RequestHeader};
use kafka_protocol::protocol::StrBytes;
use tokio::net::TcpStream;
use tower::Service;
use tower_kafka::client::KafkaTransportError;
use tower_kafka::{KafkaRequest, TowerKafka, client};

// Make sure to run kafka from docker-compose in root of project.
#[tokio::test]
async fn test() -> Result<(), KafkaTransportError> {
    let stream = TcpStream::connect("127.0.0.1:9092").await.unwrap();
    let client = tower_kafka::client::KafkaClient::connect(stream).await.unwrap();
    let mut tk = TowerKafka::new(client);
    let mut header = RequestHeader::default();
    header.client_id = Some(StrBytes::from_str("hi"));
    header.request_api_key = ApiKey::MetadataKey as i16;
    header.request_api_version = 12;
    let mut request = MetadataRequest::default();
    request.topics = None;
    request.allow_auto_topic_creation = true;

    tk.call((
        header,
        request,
    )).await?;


    Ok(())
}