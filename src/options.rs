// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::fmt;
use std::time::Duration;

use compact_str::CompactString;

use crate::protocol::properties::BasicProperties;
use crate::protocol::types::{FieldTable, FieldValue};

fn duration_to_millis_i64(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

//
// Queue declare options
//

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowMode {
    DropHead,
    RejectPublish,
    RejectPublishDlx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterStrategy {
    AtMostOnce,
    AtLeastOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueLeaderLocator {
    ClientLocal,
    Balanced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMode {
    Default,
    Lazy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayedRetryType {
    All,
    Failed,
    Returned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxAge(CompactString);

impl MaxAge {
    pub fn years(n: u32) -> Self {
        Self(CompactString::new(format!("{n}Y")))
    }
    pub fn months(n: u32) -> Self {
        Self(CompactString::new(format!("{n}M")))
    }
    pub fn days(n: u32) -> Self {
        Self(CompactString::new(format!("{n}D")))
    }
    pub fn hours(n: u32) -> Self {
        Self(CompactString::new(format!("{n}h")))
    }
    pub fn minutes(n: u32) -> Self {
        Self(CompactString::new(format!("{n}m")))
    }
    pub fn seconds(n: u32) -> Self {
        Self(CompactString::new(format!("{n}s")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueType {
    Classic,
    Quorum,
    Stream,
    /// Tanzu RabbitMQ delayed queue type.
    Delayed,
    /// Tanzu RabbitMQ JMS queue type.
    Jms,
    /// Custom plugin-provided queue types.
    Custom(String),
}

impl QueueType {
    pub fn as_str(&self) -> &str {
        match self {
            QueueType::Classic => "classic",
            QueueType::Quorum => "quorum",
            QueueType::Stream => "stream",
            QueueType::Delayed => "delayed",
            QueueType::Jms => "jms",
            QueueType::Custom(s) => s,
        }
    }
}

impl fmt::Display for QueueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Queue declaration options. Defaults to durable.
///
/// Non-durable non-exclusive queues (transient queues) are
/// [deprecated in RabbitMQ 4.3+](https://www.rabbitmq.com/docs/deprecated-features)
/// and will be rejected by default. Use `durable()`, `quorum()`, `stream()`,
/// or `exclusive()` instead.
#[derive(Debug, Clone)]
pub struct QueueDeclareOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    arguments: FieldTable,
}

impl Default for QueueDeclareOptions {
    fn default() -> Self {
        Self {
            durable: true,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::new(),
        }
    }
}

impl QueueDeclareOptions {
    pub fn durable() -> Self {
        Self::default()
    }

    /// Exclusive, auto-delete queue (not affected by the transient queue deprecation).
    pub fn exclusive() -> Self {
        Self {
            exclusive: true,
            auto_delete: true,
            ..Default::default()
        }
    }

    pub fn quorum() -> Self {
        Self::default().queue_type(QueueType::Quorum)
    }

    pub fn stream() -> Self {
        Self::default().queue_type(QueueType::Stream)
    }

    /// Tanzu RabbitMQ delayed queue.
    pub fn delayed() -> Self {
        Self::default().queue_type(QueueType::Delayed)
    }

    /// Tanzu RabbitMQ JMS queue.
    pub fn jms() -> Self {
        Self::default().queue_type(QueueType::Jms)
    }

    pub fn queue_type(mut self, qt: QueueType) -> Self {
        self.arguments.insert("x-queue-type", qt.as_str());
        self
    }

    // Common x-arguments

    pub fn expires(mut self, ttl: Duration) -> Self {
        assert!(!ttl.is_zero(), "x-expires must be > 0");
        self.arguments
            .insert("x-expires", duration_to_millis_i64(ttl));
        self
    }

    pub fn message_ttl(mut self, ttl: Duration) -> Self {
        self.arguments
            .insert("x-message-ttl", duration_to_millis_i64(ttl));
        self
    }

    pub fn dead_letter_exchange(mut self, exchange: &str) -> Self {
        self.arguments.insert("x-dead-letter-exchange", exchange);
        self
    }

    pub fn dead_letter_routing_key(mut self, key: &str) -> Self {
        self.arguments.insert("x-dead-letter-routing-key", key);
        self
    }

    pub fn dead_letter_strategy(mut self, strategy: DeadLetterStrategy) -> Self {
        self.arguments.insert(
            "x-dead-letter-strategy",
            match strategy {
                DeadLetterStrategy::AtMostOnce => "at-most-once",
                DeadLetterStrategy::AtLeastOnce => "at-least-once",
            },
        );
        self
    }

    pub fn max_length(mut self, n: i64) -> Self {
        self.arguments.insert("x-max-length", n);
        self
    }

    pub fn max_length_bytes(mut self, n: i64) -> Self {
        self.arguments.insert("x-max-length-bytes", n);
        self
    }

    pub fn overflow(mut self, mode: OverflowMode) -> Self {
        self.arguments.insert(
            "x-overflow",
            match mode {
                OverflowMode::DropHead => "drop-head",
                OverflowMode::RejectPublish => "reject-publish",
                OverflowMode::RejectPublishDlx => "reject-publish-dlx",
            },
        );
        self
    }

    pub fn single_active_consumer(mut self) -> Self {
        self.arguments.insert("x-single-active-consumer", true);
        self
    }

    pub fn leader_locator(mut self, locator: QueueLeaderLocator) -> Self {
        self.arguments.insert(
            "x-queue-leader-locator",
            match locator {
                QueueLeaderLocator::ClientLocal => "client-local",
                QueueLeaderLocator::Balanced => "balanced",
            },
        );
        self
    }

    // Classic queue x-arguments

    pub fn max_priority(mut self, max: u8) -> Self {
        self.arguments.insert("x-max-priority", max as i32);
        self
    }

    pub fn queue_mode(mut self, mode: QueueMode) -> Self {
        self.arguments.insert(
            "x-queue-mode",
            match mode {
                QueueMode::Default => "default",
                QueueMode::Lazy => "lazy",
            },
        );
        self
    }

    // Quorum queue x-arguments

    pub fn quorum_initial_group_size(mut self, size: u32) -> Self {
        self.arguments
            .insert("x-quorum-initial-group-size", size as i64);
        self
    }

    pub fn quorum_target_group_size(mut self, size: u32) -> Self {
        self.arguments
            .insert("x-quorum-target-group-size", size as i64);
        self
    }

    pub fn delivery_limit(mut self, limit: i64) -> Self {
        self.arguments.insert("x-delivery-limit", limit);
        self
    }

    // Stream x-arguments

    pub fn max_age(mut self, age: MaxAge) -> Self {
        self.arguments.insert("x-max-age", age.0.as_str());
        self
    }

    pub fn stream_max_segment_size_bytes(mut self, size: u64) -> Self {
        self.arguments
            .insert("x-stream-max-segment-size-bytes", size as i64);
        self
    }

    pub fn stream_filter_size_bytes(mut self, size: u8) -> Self {
        self.arguments
            .insert("x-stream-filter-size-bytes", size as i32);
        self
    }

    pub fn initial_cluster_size(mut self, size: u32) -> Self {
        self.arguments.insert("x-initial-cluster-size", size as i64);
        self
    }

    // Delayed queue x-arguments (Tanzu RabbitMQ)

    pub fn delayed_retry_type(mut self, retry_type: DelayedRetryType) -> Self {
        self.arguments.insert(
            "x-delayed-retry-type",
            match retry_type {
                DelayedRetryType::All => "all",
                DelayedRetryType::Failed => "failed",
                DelayedRetryType::Returned => "returned",
            },
        );
        self
    }

    pub fn delayed_retry_min(mut self, min: Duration) -> Self {
        self.arguments
            .insert("x-delayed-retry-min", duration_to_millis_i64(min));
        self
    }

    pub fn delayed_retry_max(mut self, max: Duration) -> Self {
        self.arguments
            .insert("x-delayed-retry-max", duration_to_millis_i64(max));
        self
    }

    // JMS queue x-arguments (Tanzu RabbitMQ)

    /// `x-selector-fields` for JMS selectors (e.g. `&["priority", "region"]`).
    pub fn selector_fields(mut self, fields: &[&str]) -> Self {
        let arr: Vec<FieldValue> = fields.iter().map(|f| FieldValue::from(*f)).collect();
        self.arguments
            .insert("x-selector-fields", FieldValue::Array(arr));
        self
    }

    /// `x-selector-field-max-bytes`.
    pub fn selector_field_max_bytes(mut self, max: i64) -> Self {
        self.arguments.insert("x-selector-field-max-bytes", max);
        self
    }

    /// `x-consumer-disconnected-timeout` (milliseconds).
    pub fn consumer_disconnected_timeout(mut self, timeout: Duration) -> Self {
        self.arguments.insert(
            "x-consumer-disconnected-timeout",
            duration_to_millis_i64(timeout),
        );
        self
    }

    // Escape hatch

    pub fn with_argument(
        mut self,
        key: impl Into<CompactString>,
        value: impl Into<FieldValue>,
    ) -> Self {
        self.arguments.insert(key, value);
        self
    }

    pub fn arguments(&self) -> &FieldTable {
        &self.arguments
    }

    pub fn into_arguments(self) -> FieldTable {
        self.arguments
    }
}

//
// Exchange declare options
//

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl ExchangeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeType::Direct => "direct",
            ExchangeType::Fanout => "fanout",
            ExchangeType::Topic => "topic",
            ExchangeType::Headers => "headers",
        }
    }
}

impl fmt::Display for ExchangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExchangeDeclareOptions {
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    arguments: FieldTable,
}

impl ExchangeDeclareOptions {
    pub fn durable() -> Self {
        Self {
            durable: true,
            ..Default::default()
        }
    }

    pub fn alternate_exchange(self, exchange: &str) -> Self {
        self.with_argument("alternate-exchange", exchange)
    }

    pub fn with_argument(
        mut self,
        key: impl Into<CompactString>,
        value: impl Into<FieldValue>,
    ) -> Self {
        self.arguments.insert(key, value);
        self
    }

    pub fn arguments(&self) -> &FieldTable {
        &self.arguments
    }

    pub fn into_arguments(self) -> FieldTable {
        self.arguments
    }
}

//
// Delete options
//

#[derive(Debug, Clone, Default)]
pub struct QueueDeleteOptions {
    pub if_unused: bool,
    pub if_empty: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ExchangeDeleteOptions {
    pub if_unused: bool,
}

//
// Consume options
//

#[derive(Debug, Clone, PartialEq)]
pub enum StreamOffset {
    First,
    Last,
    Next,
    Offset(u64),
    /// POSIX timestamp (seconds).
    Timestamp(u64),
    Interval(MaxAge),
}

#[derive(Debug, Clone, Default)]
pub struct ConsumeOptions {
    pub no_ack: bool,
    pub exclusive: bool,
    arguments: FieldTable,
}

impl ConsumeOptions {
    pub fn no_ack(mut self, no_ack: bool) -> Self {
        self.no_ack = no_ack;
        self
    }

    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.arguments.insert("x-priority", priority);
        self
    }

    pub fn stream_offset(mut self, offset: StreamOffset) -> Self {
        match offset {
            StreamOffset::First => {
                self.arguments.insert("x-stream-offset", "first");
            }
            StreamOffset::Last => {
                self.arguments.insert("x-stream-offset", "last");
            }
            StreamOffset::Next => {
                self.arguments.insert("x-stream-offset", "next");
            }
            StreamOffset::Offset(n) => {
                self.arguments.insert("x-stream-offset", n as i64);
            }
            StreamOffset::Timestamp(t) => {
                self.arguments
                    .insert("x-stream-offset", FieldValue::Timestamp(t));
            }
            StreamOffset::Interval(age) => {
                self.arguments.insert("x-stream-offset", age.0.as_str());
            }
        }
        self
    }

    pub fn stream_filter(mut self, filter: &str) -> Self {
        self.arguments.insert("x-stream-filter", filter);
        self
    }

    pub fn stream_match_unfiltered(mut self) -> Self {
        self.arguments.insert("x-stream-match-unfiltered", true);
        self
    }

    pub fn consumer_timeout(mut self, timeout: Duration) -> Self {
        self.arguments
            .insert("x-consumer-timeout", duration_to_millis_i64(timeout));
        self
    }

    /// `x-jms-selector` expression (Tanzu RabbitMQ).
    pub fn jms_selector(mut self, selector: &str) -> Self {
        self.arguments.insert("x-jms-selector", selector);
        self
    }

    pub fn with_argument(
        mut self,
        key: impl Into<CompactString>,
        value: impl Into<FieldValue>,
    ) -> Self {
        self.arguments.insert(key, value);
        self
    }

    pub fn arguments(&self) -> &FieldTable {
        &self.arguments
    }

    pub fn into_arguments(self) -> FieldTable {
        self.arguments
    }
}

//
// Publish options
//

#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    pub mandatory: bool,
    pub properties: BasicProperties,
}

impl PublishOptions {
    /// Mandatory publish: the broker will return the message if it
    /// cannot be routed to any queue or stream.
    pub fn mandatory() -> Self {
        Self {
            mandatory: true,
            ..Default::default()
        }
    }
}

//
// Headers exchange binding arguments
//

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadersMatch {
    All,
    Any,
    AllWithX,
    AnyWithX,
}

#[derive(Debug, Clone, Default)]
pub struct BindingArguments {
    inner: FieldTable,
}

impl BindingArguments {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn match_mode(mut self, mode: HeadersMatch) -> Self {
        self.inner.insert(
            "x-match",
            match mode {
                HeadersMatch::All => "all",
                HeadersMatch::Any => "any",
                HeadersMatch::AllWithX => "all-with-x",
                HeadersMatch::AnyWithX => "any-with-x",
            },
        );
        self
    }

    pub fn header(mut self, name: impl Into<CompactString>, value: impl Into<FieldValue>) -> Self {
        self.inner.insert(name, value);
        self
    }

    pub fn with_argument(
        mut self,
        key: impl Into<CompactString>,
        value: impl Into<FieldValue>,
    ) -> Self {
        self.inner.insert(key, value);
        self
    }

    pub fn into_field_table(self) -> FieldTable {
        self.inner
    }
}

impl From<BindingArguments> for FieldTable {
    fn from(b: BindingArguments) -> Self {
        b.inner
    }
}
