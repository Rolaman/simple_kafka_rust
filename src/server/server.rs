use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use log::{error, info};
use crate::server::error::ServerError;
use crate::server::service::MessageService;

pub struct KafkaServer {
    address: String,
    service: Arc<MessageService>,
}

impl KafkaServer {
    pub fn new(address: String, service: Arc<MessageService>) -> KafkaServer {
        KafkaServer { address, service }
    }

    pub fn start(&self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.address).unwrap();
        info!("Server listening on {}", self.address);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let service = self.service.clone();
                    thread::spawn(move || {
                        match service.handle(stream) {
                            Ok(_) => { info!("Finish connection") }
                            Err(e) => { error!("Error: {}", e) }
                        };
                    });
                }
                Err(e) => {
                    error!("Failed to accept a connection: {}", e);
                }
            }
        }
        Ok(())
    }
}