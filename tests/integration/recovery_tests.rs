// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::SubscribeOptions;
use bunny_rs::connection::ConnectionEvent;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

/// Force-close all connections from the broker side via rabbitmqctl.
fn force_close_connections() -> bool {
    let cmd = std::env::var("BUNNY_RS_RABBITMQCTL").unwrap_or_else(|_| "rabbitmqctl".into());
    let output = std::process::Command::new(&cmd)
        .args(["close_all_connections", "integration test: recovery"])
        .current_dir(std::env::temp_dir())
        .output();

    match output {
        Ok(o) => o.status.success(),
        Err(_) => false,
    }
}

/// Test that after a broker-forced connection close, recovery reconnects
/// and topology (queues) is replayed.
#[tokio::test]
#[ignore = "takes ~6s for recovery backoff"]
async fn test_connection_recovery_after_force_close() {
    let conn = connect().await;
    let mut events = conn.events();

    // Declare a queue so topology has something to replay
    let mut ch = conn.open_channel().await.unwrap();
    ch.queue_declare("bunny-rs.test.recovery-q", QueueDeclareOptions::default())
        .await
        .unwrap();

    // Force close from broker
    let closed = force_close_connections();
    assert!(closed, "rabbitmqctl close_all_connections failed");

    // Wait for RecoveryStarted + RecoverySucceeded events
    let timeout = std::time::Duration::from_secs(15);
    let mut saw_started = false;
    let mut saw_succeeded = false;

    let result = tokio::time::timeout(timeout, async {
        while !saw_succeeded {
            match events.recv().await {
                Ok(ConnectionEvent::RecoveryStarted) => saw_started = true,
                Ok(ConnectionEvent::RecoverySucceeded) => saw_succeeded = true,
                Ok(ConnectionEvent::RecoveryFailed(msg)) => {
                    panic!("recovery failed: {msg}");
                }
                Ok(_) => {}
                Err(e) => panic!("event channel error: {e}"),
            }
        }
    })
    .await;

    assert!(result.is_ok(), "timed out waiting for recovery");
    assert!(saw_started, "should have seen RecoveryStarted");
    assert!(saw_succeeded, "should have seen RecoverySucceeded");

    // Connection should be open again
    assert!(conn.is_open());

    // The recovered connection should be usable
    let mut ch2 = conn.open_channel().await.unwrap();
    ch2.basic_publish(
        "",
        "bunny-rs.test.recovery-q",
        &PublishOptions::default(),
        b"after-recovery",
    )
    .await
    .unwrap();

    ch2.queue_delete("bunny-rs.test.recovery-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    conn.close().await.unwrap();
}

/// Recovery must complete cleanly even when a consumer was active at the
/// moment the connection dropped. The recovery loop replays the consumer on
/// the temporary recovery channel, and the broker may interleave a
/// `basic.deliver` (for an unacked message that the broker requeued when it
/// noticed the dead TCP connection) with the `basic.consume-ok`. The replay
/// path must not get confused by that interleaving.
#[tokio::test]
#[ignore = "takes ~6s for recovery backoff"]
async fn recovery_completes_with_active_consumer() {
    let conn = connect().await;
    let mut events = conn.events();
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();

    let q = "bunny-rs.test.recovery-with-consumer";
    pub_ch
        .queue_declare(q, QueueDeclareOptions::default())
        .await
        .unwrap();
    pub_ch.queue_purge(q).await.unwrap();

    let mut sub = con_ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("recovery-sub"))
        .await
        .unwrap();

    // Round-trip one message to confirm the consumer is online and active.
    pub_ch
        .basic_publish("", q, &PublishOptions::default(), b"before")
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.body.as_ref(), b"before");
    d.ack().await.unwrap();

    assert!(force_close_connections());

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match events.recv().await {
                Ok(ConnectionEvent::RecoverySucceeded) => break,
                Ok(ConnectionEvent::RecoveryFailed(msg)) => panic!("recovery failed: {msg}"),
                Ok(_) => {}
                Err(e) => panic!("event channel error: {e}"),
            }
        }
    })
    .await
    .expect("recovery did not succeed");

    // Pre-existing channels and Subscriptions are stale after recovery: their
    // writer_tx clones point to the dead writer task. Open a fresh channel
    // and a fresh subscription on the recovered connection.
    drop(sub);
    let mut ch = conn.open_channel().await.unwrap();
    // Drain any unacked messages the broker requeued when the old connection
    // died (the ack we sent before force_close may not have reached the broker
    // in time).
    ch.queue_purge(q).await.unwrap();
    let mut sub2 = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("recovery-sub-2"))
        .await
        .unwrap();
    ch.publish("", q, b"after").await.unwrap();
    let d2 = tokio::time::timeout(Duration::from_secs(5), sub2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d2.body.as_ref(), b"after");
    d2.ack().await.unwrap();

    sub2.cancel().await.unwrap();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    conn.close().await.unwrap();
}
