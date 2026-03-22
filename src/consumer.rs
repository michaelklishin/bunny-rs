// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::future::Future;

use compact_str::CompactString;
use tokio::sync::oneshot;

use crate::channel::Delivery;
use crate::connection::ConnectionError;

/// Stateful message consumer. Methods are called sequentially.
pub trait Consumer: Send + 'static {
    fn handle_delivery(
        &mut self,
        delivery: Delivery,
    ) -> impl Future<Output = Result<(), ConnectionError>> + Send;

    fn handle_cancellation(&mut self, _consumer_tag: &str) -> impl Future<Output = ()> + Send {
        async {}
    }

    fn handle_consume_ok(&mut self, _consumer_tag: &str) {}

    fn handle_recovered(&mut self) {}
}

/// Handle for cancelling a `basic_consume_with` consumer.
pub struct ConsumerHandle {
    consumer_tag: CompactString,
    cancel_tx: tokio::sync::oneshot::Sender<()>,
}

impl ConsumerHandle {
    pub(crate) fn new(consumer_tag: CompactString, cancel_tx: oneshot::Sender<()>) -> Self {
        Self {
            consumer_tag,
            cancel_tx,
        }
    }

    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Stop the background consumer task.
    pub fn cancel(self) {
        let _ = self.cancel_tx.send(());
    }
}
