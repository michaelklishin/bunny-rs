// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use compact_str::CompactString;

use crate::channel::{Channel, PublishConfirm};
use crate::connection::ConnectionError;
use crate::options::{ConsumeOptions, PublishOptions, QueueDeleteOptions};
use crate::protocol::types::FieldTable;

/// Handle to a declared queue, delegating to its channel.
pub struct Queue<'a> {
    channel: &'a mut Channel,
    name: CompactString,
}

impl<'a> Queue<'a> {
    pub(crate) fn new(ch: &'a mut Channel, name: CompactString) -> Self {
        Self { channel: ch, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn bind(
        &mut self,
        exchange: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        self.channel
            .queue_bind(&self.name, exchange, routing_key, arguments)
            .await
    }

    pub async fn unbind(
        &mut self,
        exchange: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        self.channel
            .queue_unbind(&self.name, exchange, routing_key, arguments)
            .await
    }

    pub async fn consume(
        &mut self,
        consumer_tag: &str,
        opts: ConsumeOptions,
    ) -> Result<CompactString, ConnectionError> {
        self.channel
            .basic_consume(&self.name, consumer_tag, opts)
            .await
    }

    pub async fn publish(
        &mut self,
        opts: &PublishOptions,
        body: &[u8],
    ) -> Result<Option<PublishConfirm>, ConnectionError> {
        self.channel.basic_publish("", &self.name, opts, body).await
    }

    pub async fn purge(&mut self) -> Result<u32, ConnectionError> {
        self.channel.queue_purge(&self.name).await
    }

    pub async fn delete(&mut self, opts: QueueDeleteOptions) -> Result<u32, ConnectionError> {
        self.channel.queue_delete(&self.name, opts).await
    }
}
