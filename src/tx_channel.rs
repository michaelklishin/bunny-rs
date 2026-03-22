// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::ops::{Deref, DerefMut};

use crate::channel::Channel;
use crate::connection::ConnectionError;

/// Channel in transaction mode. See [`Channel::into_tx_mode`].
pub struct TxChannel {
    channel: Channel,
}

impl TxChannel {
    pub async fn commit(&mut self) -> Result<(), ConnectionError> {
        self.channel.tx_commit().await
    }

    pub async fn rollback(&mut self) -> Result<(), ConnectionError> {
        self.channel.tx_rollback().await
    }
}

impl Deref for TxChannel {
    type Target = Channel;
    fn deref(&self) -> &Channel {
        &self.channel
    }
}

impl DerefMut for TxChannel {
    fn deref_mut(&mut self) -> &mut Channel {
        &mut self.channel
    }
}

impl Channel {
    /// Enter transaction mode. Consumes self. Idempotent.
    pub async fn into_tx_mode(mut self) -> Result<TxChannel, ConnectionError> {
        self.tx_select().await?;
        Ok(TxChannel { channel: self })
    }
}
