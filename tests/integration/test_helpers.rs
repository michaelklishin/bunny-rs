// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::env;

use bunny_rs::connection::Connection;

pub fn amqp_uri() -> String {
    env::var("BUNNY_RS_AMQP_URI")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/".to_string())
}

pub async fn connect() -> Connection {
    Connection::from_uri(&amqp_uri())
        .await
        .expect("failed to connect to RabbitMQ")
}
