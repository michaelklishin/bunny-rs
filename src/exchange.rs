// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use compact_str::CompactString;

use crate::channel::{Channel, PublishConfirm};
use crate::connection::ConnectionError;
use crate::options::{ExchangeDeleteOptions, PublishOptions};
use crate::protocol::types::FieldTable;

/// Handle to a declared exchange, delegating to its channel.
pub struct Exchange<'a> {
    channel: &'a mut Channel,
    name: CompactString,
}

impl<'a> Exchange<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(ch: &'a mut Channel, name: CompactString) -> Self {
        Self { channel: ch, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn publish(
        &mut self,
        routing_key: &str,
        opts: &PublishOptions,
        body: &[u8],
    ) -> Result<Option<PublishConfirm>, ConnectionError> {
        self.channel
            .basic_publish(&self.name, routing_key, opts, body)
            .await
    }

    pub async fn bind(
        &mut self,
        source: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        self.channel
            .exchange_bind(&self.name, source, routing_key, arguments)
            .await
    }

    pub async fn unbind(
        &mut self,
        source: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        self.channel
            .exchange_unbind(&self.name, source, routing_key, arguments)
            .await
    }

    pub async fn delete(&mut self, opts: ExchangeDeleteOptions) -> Result<(), ConnectionError> {
        self.channel.exchange_delete(&self.name, opts).await
    }
}
