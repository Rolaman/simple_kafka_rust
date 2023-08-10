use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ClientMessage {
    InitClient { group: String },
    CreateTopic { name: String, retention: u128 },
    Subscribe { name: String },
    Unsubscribe { name: String },
    SendMessage { key: String, msg: String, topic: String },
    Commit { key: String, topic: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub key: String,
    pub msg: String,
    pub topic: String,
    pub timestamp: u128,
}