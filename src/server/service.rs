use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{Receiver, sync_channel, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use log::{info, warn};
use crate::common::message::{ClientMessage, Message};
use crate::server::error::ServerError;

pub struct MessageService {
    topics: RwLock<HashMap<String, Topic>>,
    subscriber_topics: RwLock<HashMap<String, HashSet<String>>>,
    subscriber_senders: RwLock<HashMap<String, SyncSender<String>>>,
    subscriber_receivers: Mutex<HashMap<String, Receiver<String>>>,
}

pub struct Topic {
    name: String,
    subscribers_offset: HashMap<String, u128>,
    messages: VecDeque<Message>,
    retention: u128,
    message_timestamp_map: HashMap<String, u128>,
    waiting_subscribers: HashSet<String>,
    uncommitted_messages: HashMap<String, HashSet<String>>,
}

impl MessageService {
    pub fn new() -> MessageService {
        MessageService {
            topics: Default::default(),
            subscriber_topics: Default::default(),
            subscriber_senders: Default::default(),
            subscriber_receivers: Default::default(),
        }
    }

    pub fn handle(self: Arc<Self>, stream : TcpStream) -> Result<(), ServerError> {
        let mut buf = String::new();
        let (mut reader_stream, mut writer_stream) = stream.try_clone()
            .map(|clone| (BufReader::new(clone), BufWriter::new(stream)))?;
        reader_stream.read_line(&mut buf).map_err(ServerError::ConnectionError)?;
        let message: ClientMessage = serde_json::from_str(&buf)?;
        let group = match message {
            ClientMessage::InitClient { group } => self.clone().register_client(group),
            _ => Err(ServerError::UnexpectedMessage("Expect init client request".to_string())),
        }?;
        info!("Init new client {}", group);
        let arc_self = Arc::new(self);
        let arc_self1 = arc_self.clone();
        let arc_self2 = arc_self.clone();
        let group1 = group.clone();
        let group2 = group.clone();
        let handle_listen = thread::spawn(move || {
            arc_self1.listen(group1, &mut reader_stream)
        });
        let handle_send = thread::spawn(move || {
            arc_self2.process_send(group2, &mut writer_stream);
        });
        handle_listen.join().unwrap();
        handle_send.join().unwrap();
        Ok(())
    }

    fn process_send(&self, id: String, buf: &mut BufWriter<TcpStream>) {
        loop {
            let result = self.process_send_block(id.clone(), buf);
            if result.is_err() {
                warn!("Can't send message to client: {}", result.unwrap_err());
                sleep(Duration::from_secs(1));
            }
        }
    }

    fn process_send_block(&self, id: String, buf: &mut BufWriter<TcpStream>) -> Result<(), ServerError>  {
        let rxs = self.subscriber_receivers.lock().unwrap();
        match rxs.get(&id) {
            Some(rx) => {
                let msg = rx.recv()?;
                buf.write_all(msg.as_bytes()).map_err(|e| ServerError::ConnectionError(e))?;
                buf.write_all(b"\n")?;
                buf.flush()?;
                Ok(())
            }
            None => {
                info!("No active subscriber receivers, waiting");
                sleep(Duration::from_secs(1));
                Ok(())
            }
        }
    }

    fn listen(&self, id: String, buf: &mut BufReader<TcpStream>) {
        loop {
            let result = self.listen_block(id.clone(), buf);
            if result.is_err() {
                warn!("Can't read client message {}", result.unwrap_err())
            }
        }
    }

    fn listen_block(&self, group: String, buf: &mut BufReader<TcpStream>) -> Result<(), ServerError> {
        let mut message_buf = String::new();
        buf.read_line(&mut message_buf)?;
        if message_buf.is_empty() {
            return Ok(())
        }
        let message: ClientMessage = serde_json::from_str(&message_buf)?;
        info!("Got client message {:?}", message);
        match message {
            ClientMessage::InitClient { .. } => Err(ServerError::UnexpectedMessage("Already inited".to_string())),
            ClientMessage::CreateTopic { name, retention } => self.create_topic(name, retention),
            ClientMessage::Subscribe { name } => self.subscribe(name, group),
            ClientMessage::Unsubscribe { name } => self.unsubscribe(name, group),
            ClientMessage::SendMessage { key, msg, topic } => self.send(key, msg, topic, group),
            ClientMessage::Commit { key, topic } => self.commit(key, topic, group)
        }
    }

    fn register_client(&self, group: String) -> Result<String, ServerError> {
        let mut result = self.subscriber_topics.write().unwrap();
        result.insert(group.clone(), HashSet::new());
        Ok(group)
    }

    fn create_topic(&self, name: String, retention: u128) -> Result<(), ServerError> {
        let mut topics_guard = self.topics.write().unwrap();
        if !topics_guard.contains_key(&name) {
            let topic = Topic {
                name: name.clone(),
                subscribers_offset: Default::default(),
                messages: VecDeque::new(),
                retention,
                message_timestamp_map: Default::default(),
                waiting_subscribers: Default::default(),
                uncommitted_messages: Default::default(),
            };
            topics_guard.insert(name, topic);
        }
        Ok(())
    }

    pub fn subscribe(&self, topic_name: String, group: String) -> Result<(), ServerError> {
        let mut topics_guard = self.topics.write().unwrap();
        let topic = topics_guard.get_mut(&topic_name)
            .ok_or(ServerError::MissingTopicError)?;

        topic.subscribers_offset.insert(group.clone(), 0);
        let mut sub_topics = self.subscriber_topics.write().unwrap();
        let topics = sub_topics.get_mut(&group)
            .ok_or(ServerError::MissingSubscriberError)?;
        topics.insert(topic_name);
        info!("Subscribe client {}", group);
        self.send_first_missing_messages(topic, group.clone(), 0)?;
        let (tx, rx) = sync_channel(1);
        self.subscriber_senders.write().unwrap().insert(group.clone(), tx);
        self.subscriber_receivers.lock().unwrap().insert(group.clone(), rx);
        Ok(())
    }

    fn unsubscribe(&self, topic_name: String, group: String) -> Result<(), ServerError> {
        let mut topics_guard = self.topics.write().unwrap();
        let topic = topics_guard.get_mut(&topic_name)
            .ok_or(ServerError::MissingTopicError)?;
        topic.subscribers_offset.remove(&group);
        let mut sub_topics = self.subscriber_topics.write().unwrap();
        sub_topics.remove(&group);
        self.subscriber_senders.write().unwrap().remove(&group);
        self.subscriber_receivers.lock().unwrap().remove(&group);
        Ok(())
    }

    fn commit(&self, key: String, topic_name: String, group: String) -> Result<(), ServerError> {
        let sub_topics = self.subscriber_topics.read().unwrap();
        let sub_topics = sub_topics.get(&group)
            .ok_or(ServerError::MissingSubscriberError)?;
        if !sub_topics.contains(&topic_name) {
            return Err(ServerError::MissingTopicError)
        }
        let mut topics = self.topics.write().unwrap();
        let topic = topics.get_mut(&topic_name).ok_or(ServerError::MissingTopicError)?;
        let timestamp = topic.message_timestamp_map.get(&key).ok_or(ServerError::MissingMessageError)?;
        topic.subscribers_offset.insert(group.clone(), timestamp.clone());
        topic.uncommitted_messages.get_mut(&group).unwrap().remove(&key);
        info!("Commit message with key {}", key);
        self.send_first_missing_messages(topic, group, timestamp.clone())
    }

    fn send_first_missing_messages(&self, topic: &mut Topic, group: String, offset: u128) -> Result<(), ServerError>  {
        let min_time = MessageService::current_time()? - topic.retention;
        let uncommited_opt = topic.uncommitted_messages.get(&group);
        let msg_opt = topic.messages
            .iter()
            .find(|message| {
                let not_pending = !uncommited_opt.map_or(false, |set| set.contains(&message.key));
                message.timestamp >= min_time
                    && message.timestamp > offset
                    && not_pending
            });
        match msg_opt {
            Some(msg) => {
                if !topic.uncommitted_messages.contains_key(&group) {
                    topic.uncommitted_messages.insert(group.clone(), HashSet::new());
                }
                topic.uncommitted_messages.get_mut(&group).unwrap().insert(msg.key.clone());
                info!("Sending message {:?}", msg);
                self.subscriber_senders.read().unwrap().get(&group)
                    .ok_or(ServerError::MissingSubscriberError)?
                    .send(serde_json::to_string(msg)?)?;
            }
            None => {
                info!("No available messages in topic {}", topic.name);
                topic.waiting_subscribers.insert(group);
            }
        };
        Ok(())
    }

    fn send(&self, key: String, msg: String, topic_name: String, group: String) -> Result<(), ServerError> {
        let mut topics = self.topics.write().unwrap();
        let topic = topics.get_mut(&topic_name).ok_or(ServerError::MissingTopicError)?;
        let timestamp = MessageService::current_time()?;
        let message = Message {
            key: key.clone(),
            msg,
            topic: topic_name.clone(),
            timestamp,
        };
        topic.messages.push_back(message);
        topic.message_timestamp_map.insert(key, timestamp);
        self.send_to_waiting_groups(topic, timestamp);
        Ok(())
    }

    fn send_to_waiting_groups(&self, topic: &mut Topic, timestamp: u128) {
        let subscribers: Vec<String> = topic.waiting_subscribers.iter().cloned().collect();
        for group in subscribers {
            match self.send_first_missing_messages(topic, group, timestamp - 1) {
                Ok(_) => {
                    info!("Sent new message of topic {} to waiting groups", topic.name)
                }
                Err(err) => {
                    warn!("Can't send new message {}", err)
                }
            }
        }
    }

    fn current_time() -> Result<u128, ServerError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| {
                d.as_millis()
            }).map_err(ServerError::SystemError)
    }
}