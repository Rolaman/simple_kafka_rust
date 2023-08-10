use std::env;
use std::thread::sleep;
use std::time::Duration;
use simple_kafka_rust::client::client::KafkaClient;

fn main() {
    env_logger::init();
    let address = env::var("HOST").unwrap_or("127.0.0.1:8001".to_string());
    let group = env::var("GROUP").unwrap_or("default".to_string());

    let mut client = KafkaClient::new(address.as_str(), group.as_str());
    client.connect().unwrap() ;

    client.create_topic("new_topic1", 1000000000).unwrap();
    client.subscribe("new_topic1").unwrap();

    client.send_message("100", "new_topic1").unwrap();
    sleep(Duration::from_secs(1));
    client.send_message("101", "new_topic1").unwrap();
    sleep(Duration::from_secs(1));
    client.send_message("102", "new_topic1").unwrap();
    sleep(Duration::from_secs(1));
    client.send_message("103", "new_topic1").unwrap();
    sleep(Duration::from_secs(1));
    client.send_message("104", "new_topic1").unwrap();
    sleep(Duration::from_secs(5));
    client.send_message("105", "new_topic1").unwrap();

    sleep(Duration::from_secs(10));
}