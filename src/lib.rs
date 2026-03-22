// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

pub mod channel;
pub mod confirm_channel;
pub mod connection;
pub mod consumer;
pub mod credentials;
pub mod errors;
pub mod exchange;
pub mod options;
pub mod protocol;
pub mod queue;
pub mod transport;
pub mod tx_channel;

// Re-export the types users need most so they don't have to dig into submodules.
pub use channel::{
    AmqpReplyCode, Channel, ChannelEvent, ConsumerTag, Delivery, PublishConfirm, QueueInfo,
    ResourceName, ReturnedMessage,
};
pub use confirm_channel::ConfirmChannel;
pub use connection::endpoint::{AddressResolver, Endpoint};
pub use connection::{
    Connection, ConnectionError, ConnectionEvent, ConnectionOptions, TopologyRecoveryFilter,
};
pub use consumer::{Consumer, ConsumerHandle};
pub use options::{
    ConsumeOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, ExchangeType, PublishOptions,
    QueueDeclareOptions, QueueDeleteOptions, QueueType,
};
pub use protocol::properties::{BasicProperties, DeliveryMode};
pub use protocol::types::{FieldTable, FieldValue};
pub use tx_channel::TxChannel;
