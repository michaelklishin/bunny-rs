// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::ChannelEvent;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

/// Soft error: declaring a queue with incompatible flags triggers PRECONDITION_FAILED
/// which closes the channel but not the connection.
#[tokio::test]
async fn test_soft_error_closes_channel_not_connection() {
    let conn = connect().await;

    // Declare a durable queue on channel 1
    let mut ch1 = conn.open_channel().await.unwrap();
    ch1.queue_declare("bunny-rs.test.soft-error", QueueDeclareOptions::durable())
        .await
        .unwrap();

    // Try to re-declare it as exclusive on channel 2 — should fail
    let mut ch2 = conn.open_channel().await.unwrap();
    let mut events = ch2.events();
    let result = ch2
        .queue_declare("bunny-rs.test.soft-error", QueueDeclareOptions::exclusive())
        .await;
    assert!(result.is_err(), "incompatible redeclare should fail");

    // Channel 2 should receive a Closed event
    match tokio::time::timeout(std::time::Duration::from_secs(2), events.recv()).await {
        Ok(Ok(ChannelEvent::Closed {
            initiated_by_server,
            code,
            ..
        })) => {
            assert!(initiated_by_server);
            assert!(code >= 400);
        }
        other => panic!("expected Closed event, got: {other:?}"),
    }

    // Connection should still be open
    assert!(conn.is_open());

    // Channel 1 should still work
    ch1.queue_delete("bunny-rs.test.soft-error", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch1.close().await.unwrap();
    conn.close().await.unwrap();
}

/// RPC operations on a channel that was closed by the broker should return errors.
#[tokio::test]
async fn test_rpc_on_closed_channel_fails() {
    let conn = connect().await;

    let mut ch = conn.open_channel().await.unwrap();
    ch.queue_declare(
        "bunny-rs.test.closed-ch-ops",
        QueueDeclareOptions::durable(),
    )
    .await
    .unwrap();

    // Trigger a channel error on ch2
    let mut ch2 = conn.open_channel().await.unwrap();
    let _ = ch2
        .queue_declare(
            "bunny-rs.test.closed-ch-ops",
            QueueDeclareOptions::exclusive(),
        )
        .await;

    // Small wait for the close to propagate
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // An RPC on the dead channel should fail (queue_declare waits for a response)
    let result = ch2
        .queue_declare(
            "bunny-rs.test.some-other-queue",
            QueueDeclareOptions::default(),
        )
        .await;
    assert!(result.is_err(), "RPC on closed channel should fail");

    ch.queue_delete("bunny-rs.test.closed-ch-ops", QueueDeleteOptions::default())
        .await
        .unwrap();
    conn.close().await.unwrap();
}

/// Multiple channels can coexist and one's error doesn't affect others.
#[tokio::test]
async fn test_channel_isolation() {
    let conn = connect().await;
    let mut ch1 = conn.open_channel().await.unwrap();
    let mut ch2 = conn.open_channel().await.unwrap();
    let mut ch3 = conn.open_channel().await.unwrap();

    ch1.queue_declare("bunny-rs.test.ch-isolation", QueueDeclareOptions::default())
        .await
        .unwrap();

    // Kill ch2 with a bad declare
    let _ = ch2
        .queue_declare(
            "bunny-rs.test.ch-isolation",
            QueueDeclareOptions::exclusive(),
        )
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // ch1 and ch3 should still work
    ch1.basic_publish(
        "",
        "bunny-rs.test.ch-isolation",
        &PublishOptions::default(),
        b"from-ch1",
    )
    .await
    .unwrap();

    ch3.basic_publish(
        "",
        "bunny-rs.test.ch-isolation",
        &PublishOptions::default(),
        b"from-ch3",
    )
    .await
    .unwrap();

    ch1.queue_delete("bunny-rs.test.ch-isolation", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch1.close().await.unwrap();
    ch3.close().await.unwrap();
    conn.close().await.unwrap();
}
