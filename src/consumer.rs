// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::future::Future;

use compact_str::CompactString;
use tokio::sync::oneshot;

use crate::channel::Delivery;
use crate::connection::{ConnectionError, WriterCommand, WriterTx};
use crate::protocol::frame::Frame;
use crate::protocol::method::{BasicCancelArgs, Method};

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
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    channel_id: u16,
    writer_tx: WriterTx,
}

impl ConsumerHandle {
    pub(crate) fn new(
        consumer_tag: CompactString,
        cancel_tx: oneshot::Sender<()>,
        channel_id: u16,
        writer_tx: WriterTx,
    ) -> Self {
        Self {
            consumer_tag,
            cancel_tx: Some(cancel_tx),
            channel_id,
            writer_tx,
        }
    }

    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Stop the background consumer task.
    pub fn cancel(mut self) {
        self.stop_task();
        self.send_basic_cancel();
    }

    fn stop_task(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }

    fn send_basic_cancel(&self) {
        let frame = Frame::Method(
            self.channel_id,
            Box::new(Method::BasicCancel(Box::new(BasicCancelArgs {
                consumer_tag: self.consumer_tag.clone(),
                nowait: true,
            }))),
        );
        let _ = self.writer_tx.try_send(WriterCommand::SendFrame(frame));
    }
}

impl Drop for ConsumerHandle {
    fn drop(&mut self) {
        self.stop_task();
        self.send_basic_cancel();
    }
}
