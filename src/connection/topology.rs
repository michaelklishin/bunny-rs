// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use compact_str::CompactString;

use crate::options::{ConsumeOptions, ExchangeDeclareOptions, QueueDeclareOptions};
use crate::protocol::types::FieldTable;

/// Recorded topology for automatic recovery.
#[derive(Clone, Default)]
pub struct TopologyRegistry {
    pub exchanges: Vec<RecordedExchange>,
    pub queues: Vec<RecordedQueue>,
    pub queue_bindings: Vec<RecordedQueueBinding>,
    pub exchange_bindings: Vec<RecordedExchangeBinding>,
    pub consumers: Vec<RecordedConsumer>,
}

#[derive(Clone)]
pub struct RecordedExchange {
    pub name: CompactString,
    pub kind: CompactString,
    pub opts: ExchangeDeclareOptions,
}

#[derive(Clone)]
pub struct RecordedQueue {
    pub name: CompactString,
    pub opts: QueueDeclareOptions,
    pub server_named: bool,
}

#[derive(Clone)]
pub struct RecordedQueueBinding {
    pub queue: CompactString,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub arguments: FieldTable,
}

#[derive(Clone)]
pub struct RecordedExchangeBinding {
    pub destination: CompactString,
    pub source: CompactString,
    pub routing_key: CompactString,
    pub arguments: FieldTable,
}

#[derive(Clone)]
pub struct RecordedConsumer {
    pub channel_id: u16,
    pub queue: CompactString,
    pub consumer_tag: CompactString,
    pub opts: ConsumeOptions,
}

impl TopologyRegistry {
    pub fn record_exchange(&mut self, name: &str, kind: &str, opts: &ExchangeDeclareOptions) {
        // skip predeclared exchanges
        if name.is_empty() || name.starts_with("amq.") {
            return;
        }
        self.exchanges.retain(|e| e.name != name);
        self.exchanges.push(RecordedExchange {
            name: name.into(),
            kind: kind.into(),
            opts: opts.clone(),
        });
    }

    pub fn record_queue(&mut self, name: &str, opts: &QueueDeclareOptions, server_named: bool) {
        self.queues.retain(|q| q.name != name);
        self.queues.push(RecordedQueue {
            name: name.into(),
            opts: opts.clone(),
            server_named,
        });
    }

    pub fn record_queue_binding(
        &mut self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) {
        self.queue_bindings.retain(|b| {
            !(b.queue == queue && b.exchange == exchange && b.routing_key == routing_key)
        });
        self.queue_bindings.push(RecordedQueueBinding {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments: arguments.clone(),
        });
    }

    pub fn record_exchange_binding(
        &mut self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) {
        self.exchange_bindings.retain(|b| {
            !(b.destination == destination && b.source == source && b.routing_key == routing_key)
        });
        self.exchange_bindings.push(RecordedExchangeBinding {
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            arguments: arguments.clone(),
        });
    }

    pub fn record_consumer(
        &mut self,
        channel_id: u16,
        queue: &str,
        consumer_tag: &str,
        opts: &ConsumeOptions,
    ) {
        self.consumers.push(RecordedConsumer {
            channel_id,
            queue: queue.into(),
            consumer_tag: consumer_tag.into(),
            opts: opts.clone(),
        });
    }

    pub fn remove_queue(&mut self, name: &str) {
        self.queues.retain(|q| q.name != name);
        self.queue_bindings.retain(|b| b.queue != name);
        self.consumers.retain(|c| c.queue != name);
    }

    pub fn remove_exchange(&mut self, name: &str) {
        self.exchanges.retain(|e| e.name != name);
        self.queue_bindings.retain(|b| b.exchange != name);
        self.exchange_bindings
            .retain(|b| b.source != name && b.destination != name);
    }

    pub fn remove_queue_binding(&mut self, queue: &str, exchange: &str, routing_key: &str) {
        self.queue_bindings.retain(|b| {
            !(b.queue == queue && b.exchange == exchange && b.routing_key == routing_key)
        });
    }

    pub fn remove_exchange_binding(&mut self, destination: &str, source: &str, routing_key: &str) {
        self.exchange_bindings.retain(|b| {
            !(b.destination == destination && b.source == source && b.routing_key == routing_key)
        });
    }

    pub fn remove_consumer(&mut self, consumer_tag: &str) {
        self.consumers.retain(|c| c.consumer_tag != consumer_tag);
    }

    pub fn clear(&mut self) {
        self.exchanges.clear();
        self.queues.clear();
        self.queue_bindings.clear();
        self.exchange_bindings.clear();
        self.consumers.clear();
    }

    /// Rename a queue across queues, bindings, and consumers.
    pub fn update_queue_name(&mut self, old: &str, new: &str) {
        for q in &mut self.queues {
            if q.name == old {
                q.name = new.into();
            }
        }
        for b in &mut self.queue_bindings {
            if b.queue == old {
                b.queue = new.into();
            }
        }
        for c in &mut self.consumers {
            if c.queue == old {
                c.queue = new.into();
            }
        }
    }

    /// Rename a consumer tag.
    pub fn update_consumer_tag(&mut self, old: &str, new: &str) {
        for c in &mut self.consumers {
            if c.consumer_tag == old {
                c.consumer_tag = new.into();
            }
        }
    }
}
