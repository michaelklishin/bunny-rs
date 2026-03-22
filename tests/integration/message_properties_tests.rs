// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::protocol::properties::{BasicProperties, DeliveryMode};
use bunny_rs::protocol::types::{FieldTable, FieldValue};

#[tokio::test]
async fn test_message_properties_round_trip() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.props", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.props").await.unwrap();

    let mut headers = FieldTable::new();
    headers.insert("x-custom", "header-value");
    headers.insert("x-count", FieldValue::I64(42));

    let props = BasicProperties::default()
        .content_type("application/json")
        .content_encoding("utf-8")
        .correlation_id("corr-123")
        .reply_to("reply.queue")
        .message_id("msg-456")
        .message_type("order.created")
        .app_id("bunny-rs-test")
        .user_id("guest")
        .timestamp(1700000000)
        .priority(5)
        .persistent()
        .expiration("60000")
        .headers(headers);

    ch.basic_publish(
        "",
        "bunny-rs.test.props",
        &PublishOptions {
            properties: props,
            ..Default::default()
        },
        b"{}",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let delivery = ch
        .basic_get("bunny-rs.test.props", true)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        delivery.properties().get_content_type(),
        Some("application/json")
    );
    assert_eq!(delivery.properties().get_content_encoding(), Some("utf-8"));
    assert_eq!(delivery.properties().get_correlation_id(), Some("corr-123"));
    assert_eq!(delivery.properties().get_reply_to(), Some("reply.queue"));
    assert_eq!(delivery.properties().get_message_id(), Some("msg-456"));
    assert_eq!(
        delivery.properties().get_message_type(),
        Some("order.created")
    );
    assert_eq!(delivery.properties().get_app_id(), Some("bunny-rs-test"));
    assert_eq!(delivery.properties().get_user_id(), Some("guest"));
    assert_eq!(delivery.properties().get_timestamp(), Some(1700000000));
    assert_eq!(delivery.properties().get_priority(), Some(5));
    assert_eq!(
        delivery.properties().get_delivery_mode(),
        Some(DeliveryMode::Persistent)
    );
    assert_eq!(delivery.properties().get_expiration(), Some("60000"));

    let h = delivery.properties().get_headers().unwrap();
    assert_eq!(
        h.get("x-custom"),
        Some(&FieldValue::LongString("header-value".into()))
    );
    assert_eq!(h.get("x-count"), Some(&FieldValue::I64(42)));

    ch.queue_delete("bunny-rs.test.props", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_persistent_delivery_mode() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.persistent", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.persistent").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.persistent",
        &PublishOptions {
            properties: BasicProperties::default().persistent(),
            ..Default::default()
        },
        b"durable message",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let delivery = ch
        .basic_get("bunny-rs.test.persistent", true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivery.properties().get_delivery_mode(),
        Some(DeliveryMode::Persistent)
    );

    ch.queue_delete("bunny-rs.test.persistent", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_minimal_properties() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.min-props", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.min-props").await.unwrap();

    // publish with no properties at all
    ch.basic_publish(
        "",
        "bunny-rs.test.min-props",
        &PublishOptions::default(),
        b"bare",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let delivery = ch
        .basic_get("bunny-rs.test.min-props", true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), b"bare");
    assert!(delivery.properties().get_content_type().is_none());
    assert!(delivery.properties().get_headers().is_none());

    ch.queue_delete("bunny-rs.test.min-props", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
