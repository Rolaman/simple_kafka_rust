use std::io::Error;
use std::sync::mpsc::{RecvError, SendError};
use std::time::SystemTimeError;

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("connection error: {0}")]
    ConnectionError(#[from] Error),
    #[error("deserialization error: {0}")]
    DeserializationError(#[from] serde_json::Error),
    #[error("unexpected message: {0}")]
    UnexpectedMessage(String),
    #[error("topic already exists")]
    TopicDuplication,
    #[error("system error: {0}")]
    SystemError(#[from] SystemTimeError),
    #[error("missing topic")]
    MissingTopicError,
    #[error("missing subscriber")]
    MissingSubscriberError,
    #[error("channel receiver error: {0}")]
    SystemRecvError(#[from] RecvError),
    #[error("can't send message: {0}")]
    MessageSenderError(#[from] SendError<String>),
    #[error("missing message")]
    MissingMessageError,
}