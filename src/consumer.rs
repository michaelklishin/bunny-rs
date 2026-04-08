// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

//! High-level consumer types.
//!
//! [`Consumer`] is the value users hold to receive deliveries from a queue
//! without borrowing the originating [`Channel`]. Construct one via
//! [`Queue::subscribe`](crate::queue::Queue::subscribe).
//!
//! [`DeliveryHandler`] is the trait-object callback path used by
//! [`Channel::basic_consume_with`](crate::Channel::basic_consume_with) for
//! stateful long-lived workers. [`ConsumerHandle`] is the cancellable handle
//! returned by both that method and
//! [`Queue::subscribe_with`](crate::queue::Queue::subscribe_with).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use compact_str::CompactString;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{Stream, StreamExt};

use crate::channel::{Acker, ChannelInner, Delivery, RawDelivery};
use crate::connection::{ConnectionError, WriterCommand, WriterTx};
use crate::options::ConsumeOptions;
use crate::protocol::frame::Frame;
use crate::protocol::method::{BasicCancelArgs, Method};

/// Stateful message handler. Methods are called sequentially by the dispatcher
/// task spawned by
/// [`Channel::basic_consume_with`](crate::Channel::basic_consume_with).
pub trait DeliveryHandler: Send + 'static {
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

/// Options for [`Queue::subscribe`](crate::queue::Queue::subscribe).
///
/// Defaults to manual acknowledgement; see the [RabbitMQ doc guide on consumer
/// acknowledgements](https://www.rabbitmq.com/docs/confirms). Use
/// [`auto_ack`](Self::auto_ack) for auto-ack mode.
#[derive(Debug, Clone, Default)]
pub struct SubscribeOptions {
    pub consume: ConsumeOptions,
    pub consumer_tag: Option<CompactString>,
    pub prefetch: Option<u16>,
}

impl SubscribeOptions {
    pub fn manual_ack() -> Self {
        Self::default()
    }

    /// Auto-acknowledged subscription. The broker considers a delivery acked
    /// as soon as it is sent: messages can be lost on consumer crash.
    pub fn auto_ack() -> Self {
        Self {
            consume: ConsumeOptions::default().no_ack(true),
            ..Self::default()
        }
    }

    pub fn exclusive(mut self) -> Self {
        self.consume = self.consume.exclusive(true);
        self
    }

    /// Set an explicit consumer tag. If unset, a client-side tag is generated.
    pub fn consumer_tag(mut self, tag: impl Into<CompactString>) -> Self {
        self.consumer_tag = Some(tag.into());
        self
    }

    /// Issue a `basic.qos(prefetch_count=n)` before starting the consumer.
    pub fn prefetch(mut self, n: u16) -> Self {
        self.prefetch = Some(n);
        self
    }

    /// Replace the underlying [`ConsumeOptions`] for full control (stream
    /// offsets, consumer priority, JMS selectors, etc). Overwrites the entire
    /// `ConsumeOptions`, including any `no_ack` flag set by
    /// [`manual_ack`](Self::manual_ack) / [`auto_ack`](Self::auto_ack).
    pub fn with_consume(mut self, consume: ConsumeOptions) -> Self {
        self.consume = consume;
        self
    }
}

/// A live consumer yielding [`Delivery`] values from a queue.
///
/// Does not borrow the originating [`Channel`], so the channel can publish,
/// declare, and host other consumers concurrently. Implements [`Stream`] so
/// users can drive it with [`StreamExt`] combinators (`next`, `try_next`,
/// `filter_map`, ...) or with the plain [`recv`](Self::recv) method.
///
/// Dropping a `Consumer` sends `basic.cancel` to the broker (best-effort,
/// no-wait).
///
/// The internal delivery buffer is unbounded; a slow consumer will not apply
/// backpressure to the broker. Set [`SubscribeOptions::prefetch`] to bound the
/// number of in-flight unacknowledged messages.
pub struct Consumer {
    consumer_tag: CompactString,
    channel_id: u16,
    writer_tx: WriterTx,
    channel_inner: Arc<Mutex<ChannelInner>>,
    inner: ConsumerInner,
    acker: Acker,
}

enum ConsumerInner {
    Active(UnboundedReceiverStream<RawDelivery>),
    Cancelled,
}

impl Consumer {
    pub(crate) fn new(
        consumer_tag: CompactString,
        channel_id: u16,
        writer_tx: WriterTx,
        channel_inner: Arc<Mutex<ChannelInner>>,
        delivery_rx: mpsc::UnboundedReceiver<RawDelivery>,
        acker: Acker,
    ) -> Self {
        Self {
            consumer_tag,
            channel_id,
            writer_tx,
            channel_inner,
            inner: ConsumerInner::Active(UnboundedReceiverStream::new(delivery_rx)),
            acker,
        }
    }

    /// The consumer tag assigned by the broker (or supplied by the caller).
    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Receive the next delivery, or `None` after [`cancel`](Self::cancel) /
    /// drop has drained the buffer.
    pub async fn recv(&mut self) -> Option<Delivery> {
        match &mut self.inner {
            ConsumerInner::Active(stream) => {
                let raw = stream.next().await?;
                Some(Delivery::from_raw(raw, self.acker.clone()))
            }
            ConsumerInner::Cancelled => None,
        }
    }

    /// Send `basic.cancel` to the broker and stop yielding deliveries.
    /// Idempotent. Returns `NotConnected` if the writer is gone.
    pub async fn cancel(mut self) -> Result<(), ConnectionError> {
        if matches!(self.inner, ConsumerInner::Cancelled) {
            return Ok(());
        }
        self.inner = ConsumerInner::Cancelled;
        // Reclaim the consumers map entry so re-subscribing with the same tag
        // does not see a stale sender.
        self.channel_inner
            .lock()
            .await
            .consumers
            .remove(self.consumer_tag.as_str());
        self.writer_tx
            .send(WriterCommand::SendFrame(self.cancel_frame()))
            .await
            .map_err(|_| ConnectionError::NotConnected)
    }

    fn cancel_frame(&self) -> Frame {
        Frame::Method(
            self.channel_id,
            Box::new(Method::BasicCancel(Box::new(BasicCancelArgs {
                consumer_tag: self.consumer_tag.clone(),
                nowait: true,
            }))),
        )
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // Best-effort cleanup: Drop cannot await, so we use try_lock and
        // try_send. If the lock is contended, the opportunistic path in
        // dispatch_delivery reclaims the entry on the next delivery.
        if matches!(self.inner, ConsumerInner::Active(_)) {
            let frame = self.cancel_frame();
            self.inner = ConsumerInner::Cancelled;
            if let Ok(mut guard) = self.channel_inner.try_lock() {
                guard.consumers.remove(self.consumer_tag.as_str());
            }
            let _ = self.writer_tx.try_send(WriterCommand::SendFrame(frame));
        }
    }
}

impl Stream for Consumer {
    type Item = Delivery;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.inner {
            ConsumerInner::Active(stream) => match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(raw)) => {
                    Poll::Ready(Some(Delivery::from_raw(raw, self.acker.clone())))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            ConsumerInner::Cancelled => Poll::Ready(None),
        }
    }
}

/// Handle for cancelling a [`DeliveryHandler`]-based background consumer
/// (returned by [`Channel::basic_consume_with`](crate::Channel::basic_consume_with)
/// and [`Queue::subscribe_with`](crate::queue::Queue::subscribe_with)).
pub struct ConsumerHandle {
    consumer_tag: CompactString,
    cancel_tx: Option<oneshot::Sender<()>>,
    channel_id: u16,
    writer_tx: WriterTx,
    channel_inner: Arc<Mutex<ChannelInner>>,
}

impl ConsumerHandle {
    pub(crate) fn new(
        consumer_tag: CompactString,
        cancel_tx: oneshot::Sender<()>,
        channel_id: u16,
        writer_tx: WriterTx,
        channel_inner: Arc<Mutex<ChannelInner>>,
    ) -> Self {
        Self {
            consumer_tag,
            cancel_tx: Some(cancel_tx),
            channel_id,
            writer_tx,
            channel_inner,
        }
    }

    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Stop the background consumer task.
    pub fn cancel(mut self) {
        self.stop_task();
        self.send_basic_cancel();
        self.try_cleanup_consumer_entry();
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

    fn try_cleanup_consumer_entry(&self) {
        if let Ok(mut guard) = self.channel_inner.try_lock() {
            guard.consumers.remove(self.consumer_tag.as_str());
        }
    }
}

impl Drop for ConsumerHandle {
    fn drop(&mut self) {
        self.stop_task();
        self.send_basic_cancel();
        self.try_cleanup_consumer_entry();
    }
}
