// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::ops::{Deref, DerefMut};

use crate::channel::{Channel, PublishConfirm};
use crate::connection::ConnectionError;
use crate::options::PublishOptions;

/// Channel in publisher confirm mode. See [`Channel::into_confirm_mode`].
pub struct ConfirmChannel {
    channel: Channel,
}

impl ConfirmChannel {
    /// Publish and return a confirmation handle.
    pub async fn publish(
        &mut self,
        exchange: &str,
        routing_key: &str,
        opts: &PublishOptions,
        body: &[u8],
    ) -> Result<PublishConfirm, ConnectionError> {
        self.channel
            .basic_publish(exchange, routing_key, opts, body)
            .await?
            .ok_or(ConnectionError::ChannelNotOpen)
    }

    /// Block until all pending confirms resolve.
    pub async fn wait_for_confirms(&self) -> Result<(), ConnectionError> {
        self.channel.wait_for_confirms().await
    }

    /// Next publish sequence number.
    pub async fn next_publish_seq_no(&self) -> u64 {
        self.channel.next_publish_seq_no().await
    }
}

impl Deref for ConfirmChannel {
    type Target = Channel;
    fn deref(&self) -> &Channel {
        &self.channel
    }
}

impl DerefMut for ConfirmChannel {
    fn deref_mut(&mut self) -> &mut Channel {
        &mut self.channel
    }
}

impl Channel {
    /// Enter publisher confirm mode. Consumes self. Idempotent.
    pub async fn into_confirm_mode(mut self) -> Result<ConfirmChannel, ConnectionError> {
        self.confirm_select().await?;
        Ok(ConfirmChannel { channel: self })
    }

    /// Enter confirm mode with backpressure. Publishes block when
    /// `outstanding_limit` unconfirmed messages are in flight.
    pub async fn into_confirm_mode_with_tracking(
        mut self,
        outstanding_limit: usize,
    ) -> Result<ConfirmChannel, ConnectionError> {
        self.confirm_select_with_tracking(outstanding_limit).await?;
        Ok(ConfirmChannel { channel: self })
    }
}
