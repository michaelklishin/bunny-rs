// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::RecoveryConfig;
use bunny_rs::connection::{Connection, ConnectionOptions};
use bunny_rs::protocol::types::FieldTable;

#[tokio::test]
async fn test_connect_and_close() {
    let _ = tracing_subscriber::fmt::try_init();
    let conn = connect().await;
    assert!(conn.is_open());
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connect_wrong_credentials() {
    let result = Connection::from_uri("amqp://wrong:wrong@localhost:5672/").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connect_wrong_vhost() {
    let result = Connection::from_uri("amqp://guest:guest@localhost:5672/nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_custom_client_properties() {
    let mut opts = ConnectionOptions::from_uri("amqp://guest:guest@localhost:5672/").unwrap();
    let mut props = FieldTable::new();
    props.insert("custom-app", "my-service");
    opts.client_properties = Some(props);

    let conn = Connection::open(opts).await.unwrap();
    assert!(conn.is_open());
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_recovery_config_in_options() {
    use std::time::Duration;

    let opts = ConnectionOptions {
        recovery: RecoveryConfig {
            enabled: true,
            topology_recovery: true,
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(10),
            backoff_multiplier: 1.5,
            max_attempts: Some(3),
        },
        ..ConnectionOptions::from_uri("amqp://guest:guest@localhost:5672/").unwrap()
    };

    let conn = Connection::open(opts).await.unwrap();
    assert!(conn.is_open());
    conn.close().await.unwrap();
}
