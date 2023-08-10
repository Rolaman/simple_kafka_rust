use std::io::{BufRead, BufReader, BufWriter, Error, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc::{Receiver, sync_channel, SyncSender};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use log::{debug, info, warn};
use crate::client::error::ClientError;
use crate::common::message::{ClientMessage, Message};
use crate::common::message::ClientMessage::{Commit, InitClient};

pub struct KafkaClient {
    group: String,
    host: String,
    tx: Option<SyncSender<ClientMessage>>,
}

impl KafkaClient {
    pub fn new(
        server_address: &str,
        group: &str,
    ) -> KafkaClient {
        KafkaClient {
            group: group.to_string(),
            host: server_address.to_string(),
            tx: None,
        }
    }

    pub fn connect(&mut self) -> Result<(), ClientError> {
        let mut server_socket = self.host.to_socket_addrs().map_err(ClientError::ConnectError)?;
        let ready_socket = server_socket
            .next()
            .ok_or_else(||
                ClientError::ConnectError(
                    Error::new(std::io::ErrorKind::Other, "Failed to resolve server address")
                ))?;
        let stream = TcpStream::connect(ready_socket).map_err(ClientError::ConnectError)?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).map_err(ClientError::ConnectError)?;
        let (reader_stream, mut writer_stream) = stream.try_clone()
            .map(|clone| (BufReader::new(clone), BufWriter::new(stream)))?;
        info!("Connected to TcpStream: {}", self.host);
        self.send_init_client(&mut writer_stream)?;
        let (tx, rx) = sync_channel(1);
        self.tx = Some(tx.clone());
        thread::spawn(move || {
            KafkaClient::listen(reader_stream, tx.clone());
        });
        thread::spawn(move || {
            KafkaClient::send_client_messages(writer_stream, rx);
        });
        Ok(())
    }

    pub fn create_topic(&mut self, name: &str, retention: u128) -> Result<(), ClientError> {
        let tx = self.tx.as_ref().ok_or(ClientError::NoActiveChannel())?;
        tx.send(ClientMessage::CreateTopic {
            name: name.to_string(),
            retention,
        })?;
        Ok(())
    }

    pub fn subscribe(&mut self, topic: &str) -> Result<(), ClientError> {
        let tx = self.tx.as_ref().ok_or(ClientError::NoActiveChannel())?;
        tx.send(ClientMessage::Subscribe {
            name: topic.to_string(),
        })?;
        Ok(())
    }

    pub fn send_message(&mut self, msg: &str, topic: &str) -> Result<(), ClientError> {
        let tx = self.tx.as_ref().ok_or(ClientError::NoActiveChannel())?;
        tx.send(ClientMessage::SendMessage {
            key: msg.to_string(),
            msg: msg.to_string(),
            topic: topic.to_string(),
        })?;
        Ok(())
    }

    fn send_client_messages(mut buf: BufWriter<TcpStream>, rx: Receiver<ClientMessage>) {
        loop {
            let result = KafkaClient::send_block(&mut buf, &rx);
            match result {
                Ok(_) => {
                    info!("Send client message");
                }
                Err(e) => {
                    warn!("Can't send client message: {}", e)
                }
            }
        }
    }

    fn send_block(buf: &mut BufWriter<TcpStream>, rx: &Receiver<ClientMessage>) -> Result<(), ClientError> {
        let message = rx.recv()?;
        let msg = serde_json::to_string(&message)?;
        buf.write_all(msg.as_bytes())?;
        buf.write_all(b"\n")?;
        buf.flush()?;
        info!("Sent client message {:?}", message);
        Ok(())
    }

    fn listen(mut buf: BufReader<TcpStream>, sender: SyncSender<ClientMessage>) {
        loop {
            let result = KafkaClient::listen_block(&mut buf);
            match result {
                Ok(msg_opt) => {
                    match msg_opt {
                        None => {
                            debug!("No new messages, waiting");
                            sleep(Duration::from_millis(100));
                        }
                        Some(msg) => {
                            info!("Got message {}", msg.msg);
                            let commit_send_result = sender.send(Commit {
                                key: msg.key,
                                topic: msg.topic,
                            });
                            if commit_send_result.is_err() {
                                warn!("Can't commit message process: {}", commit_send_result.err().unwrap())
                            }
                        }
                    }
                }
                Err(e) => {
                    // ignore
                }
            }
        }
    }

    fn listen_block(buf: &mut BufReader<TcpStream>) -> Result<Option<Message>, ClientError> {
        let mut message_buf = String::new();
        buf.read_line(&mut message_buf)?;
        if message_buf.is_empty() {
            return Ok(None)
        }
        let message = serde_json::from_str(&message_buf)?;
        Ok(message)
    }

    fn send_init_client(&mut self, writer: &mut BufWriter<TcpStream>) -> Result<(), ClientError> {
        let msg = serde_json::to_string(&InitClient {
            group: self.group.clone(),
        })?;
        writer.write_all(msg.as_bytes()).map_err(ClientError::ConnectError)?;
        writer.write_all(b"\n").map_err(ClientError::ConnectError)?;
        writer.flush().map_err(ClientError::ConnectError)?;
        info!("Sent init client message");
        Ok(())
    }
}