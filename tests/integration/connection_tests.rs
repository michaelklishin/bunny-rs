// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::connection::Connection;

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
