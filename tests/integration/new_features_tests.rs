// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions};

#[tokio::test]
async fn test_server_properties() {
    let conn = connect().await;
    let props = conn.server_properties();
    // Every RabbitMQ node reports a "product" key
    assert!(
        props.get("product").is_some(),
        "server_properties should contain 'product', got: {:?}",
        props,
    );
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_negotiated_accessors() {
    let conn = connect().await;
    // frame_max is always > 0 after successful negotiation
    assert!(conn.frame_max() > 0);
    // heartbeat may be 0 if the server allows disabling it
    let _ = conn.heartbeat();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_blocked_accessors() {
    let conn = connect().await;
    // A fresh connection should not be blocked
    assert!(!conn.is_blocked().await);
    assert!(conn.blocked_reason().await.is_none());
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_channel_close_event() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let events = ch.events();

    // Trigger a broker-initiated channel close by re-declaring an existing
    // queue with incompatible options.
    let _ = ch
        .queue_declare("bunny_rs.test.ch_event", QueueDeclareOptions::durable())
        .await;

    // Open a second channel and try to declare the same queue as exclusive,
    // which will cause the broker to close the second channel.
    let mut ch2 = conn.open_channel().await.unwrap();
    let mut events2 = ch2.events();
    let result = ch2
        .queue_declare("bunny_rs.test.ch_event", QueueDeclareOptions::exclusive())
        .await;
    assert!(result.is_err());

    // The events receiver should have a Closed event
    match tokio::time::timeout(std::time::Duration::from_secs(2), events2.recv()).await {
        Ok(Ok(bunny_rs::ChannelEvent::Closed { code, .. })) => {
            // PRECONDITION_FAILED or RESOURCE_LOCKED
            assert!(code >= 400, "expected error code >= 400, got: {}", code);
        }
        other => {
            panic!("expected ChannelEvent::Closed, got: {:?}", other);
        }
    }

    // Clean up
    ch.queue_delete("bunny_rs.test.ch_event", Default::default())
        .await
        .unwrap();
    drop(events);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_basic_return() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    // Publish a mandatory message to the default exchange with a routing key
    // that doesn't match any queue — the broker will return it.
    let opts = PublishOptions {
        mandatory: true,
        ..Default::default()
    };
    ch.basic_publish(
        "",
        "bunny_rs.test.no_such_queue.mandatory",
        &opts,
        b"returned body",
    )
    .await
    .unwrap();

    // Should receive the returned message
    match tokio::time::timeout(std::time::Duration::from_secs(5), ch.recv_return()).await {
        Ok(Some(returned)) => {
            assert_eq!(
                returned.routing_key.as_str(),
                "bunny_rs.test.no_such_queue.mandatory"
            );
            assert_eq!(&returned.body[..], b"returned body");
            assert!(returned.reply_code > 0);
        }
        other => {
            panic!("expected ReturnedMessage, got: {:?}", other);
        }
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_confirm_backpressure() {
    let conn = connect().await;
    let ch = conn.open_channel().await.unwrap();

    // Create a confirm channel with outstanding limit of 2
    let mut cch = ch.into_confirm_mode_with_tracking(2).await.unwrap();

    // Declare a temporary queue to ensure publishes are routable
    let q = cch.temporary_queue().await.unwrap();

    // Publish 3 messages — the first two should proceed, the third
    // will block until one confirm resolves. Since the broker confirms
    // promptly, all three should complete within a reasonable timeout.
    let mut confirms = Vec::new();
    for i in 0..3u8 {
        let confirm = cch
            .publish("", &q.name, &PublishOptions::default(), &[i])
            .await
            .unwrap();
        confirms.push(confirm);
    }

    // All confirms should resolve
    for c in confirms {
        tokio::time::timeout(std::time::Duration::from_secs(5), c.wait())
            .await
            .expect("confirm timed out")
            .expect("confirm nacked");
    }

    conn.close().await.unwrap();
}
