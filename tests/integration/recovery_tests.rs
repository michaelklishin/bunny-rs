// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
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
    ch.queue_declare(
        "bunny-rs.test.recovery-q",
        QueueDeclareOptions::default(),
    )
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
