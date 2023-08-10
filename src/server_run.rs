use std::env;
use std::sync::Arc;
use log::info;
use simple_kafka_rust::server::server::KafkaServer;
use simple_kafka_rust::server::service::MessageService;

fn main() {
    env_logger::init();
    let address = env::var("HOST").unwrap_or("127.0.0.1:8001".to_string());

    let message_service = Arc::new(MessageService::new());
    let kafka_server = KafkaServer::new(address, message_service);
    info!("Starting server");
    kafka_server.start()
        .expect("Can't run server");
}