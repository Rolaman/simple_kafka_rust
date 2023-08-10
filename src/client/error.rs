use std::io::Error;
use std::sync::mpsc::{RecvError, SendError};
use crate::common::message::ClientMessage;

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("connection error: {0}")]
    ConnectError(#[from] Error),
    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("missing connection error")]
    MissingConnectError,
    #[error("system channel error")]
    SystemChannelError(#[from] RecvError),
    #[error("system channel send error")]
    SystemChannelSendError(#[from] SendError<ClientMessage>),
    #[error("no active channel error")]
    NoActiveChannel(),
}