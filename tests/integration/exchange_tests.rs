// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, PublishOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use bunny_rs::protocol::types::FieldTable;

#[tokio::test]
async fn test_declare_fanout_exchange() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.fanout",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.exchange_delete("bunny-rs.test.fanout", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_declare_topic_exchange() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.topic",
        "topic",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.exchange_delete("bunny-rs.test.topic", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_declare_headers_exchange() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.headers",
        "headers",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.exchange_delete("bunny-rs.test.headers", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_declare_durable_exchange() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.durable-x",
        "direct",
        ExchangeDeclareOptions::durable(),
    )
    .await
    .unwrap();
    ch.exchange_delete("bunny-rs.test.durable-x", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_exchange_to_exchange_binding() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.e2e.source",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "bunny-rs.test.e2e.dest",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();

    ch.exchange_bind(
        "bunny-rs.test.e2e.dest",
        "bunny-rs.test.e2e.source",
        "",
        FieldTable::new(),
    )
    .await
    .unwrap();

    // cleanup
    ch.exchange_unbind(
        "bunny-rs.test.e2e.dest",
        "bunny-rs.test.e2e.source",
        "",
        FieldTable::new(),
    )
    .await
    .unwrap();
    ch.exchange_delete("bunny-rs.test.e2e.source", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete("bunny-rs.test.e2e.dest", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_fanout_routing() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.fanout-route",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();

    // bind two queues to the same fanout
    ch.queue_declare("bunny-rs.test.fanout.q1", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_declare("bunny-rs.test.fanout.q2", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_bind(
        "bunny-rs.test.fanout.q1",
        "bunny-rs.test.fanout-route",
        "",
        FieldTable::new(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "bunny-rs.test.fanout.q2",
        "bunny-rs.test.fanout-route",
        "",
        FieldTable::new(),
    )
    .await
    .unwrap();

    ch.basic_publish(
        "bunny-rs.test.fanout-route",
        "",
        &PublishOptions::default(),
        b"fanout-msg",
    )
    .await
    .unwrap();

    // both queues should receive the message
    let d1 = ch.basic_get("bunny-rs.test.fanout.q1", true).await.unwrap();
    assert!(d1.is_some());
    assert_eq!(d1.unwrap().body.as_ref(), b"fanout-msg");

    let d2 = ch.basic_get("bunny-rs.test.fanout.q2", true).await.unwrap();
    assert!(d2.is_some());
    assert_eq!(d2.unwrap().body.as_ref(), b"fanout-msg");

    // cleanup
    ch.queue_delete("bunny-rs.test.fanout.q1", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.queue_delete("bunny-rs.test.fanout.q2", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete(
        "bunny-rs.test.fanout-route",
        ExchangeDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_topic_routing() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.topic-route",
        "topic",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();

    ch.queue_declare("bunny-rs.test.topic.q1", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_declare("bunny-rs.test.topic.q2", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_bind(
        "bunny-rs.test.topic.q1",
        "bunny-rs.test.topic-route",
        "order.*",
        FieldTable::new(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "bunny-rs.test.topic.q2",
        "bunny-rs.test.topic-route",
        "order.created",
        FieldTable::new(),
    )
    .await
    .unwrap();

    ch.basic_publish(
        "bunny-rs.test.topic-route",
        "order.created",
        &PublishOptions::default(),
        b"topic-msg",
    )
    .await
    .unwrap();

    // q1 matches "order.*" — should get the message
    let d1 = ch.basic_get("bunny-rs.test.topic.q1", true).await.unwrap();
    assert!(d1.is_some());

    // q2 matches "order.created" — should also get it
    let d2 = ch.basic_get("bunny-rs.test.topic.q2", true).await.unwrap();
    assert!(d2.is_some());

    // publish something that only q1 matches
    ch.basic_publish(
        "bunny-rs.test.topic-route",
        "order.deleted",
        &PublishOptions::default(),
        b"delete-msg",
    )
    .await
    .unwrap();

    let d1 = ch.basic_get("bunny-rs.test.topic.q1", true).await.unwrap();
    assert!(d1.is_some());

    // q2 should NOT get "order.deleted"
    let d2 = ch.basic_get("bunny-rs.test.topic.q2", true).await.unwrap();
    assert!(d2.is_none());

    // cleanup
    ch.queue_delete("bunny-rs.test.topic.q1", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.queue_delete("bunny-rs.test.topic.q2", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete(
        "bunny-rs.test.topic-route",
        ExchangeDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
