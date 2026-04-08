// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::env;
use std::sync::Once;

use bunny_rs::connection::Connection;

pub fn amqp_uri() -> String {
    env::var("BUNNY_RS_AMQP_URI")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/".to_string())
}

static TRACING: Once = Once::new();

fn init_tracing() {
    TRACING.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
            )
            .with_test_writer()
            .try_init();
    });
}

pub async fn connect() -> Connection {
    init_tracing();
    Connection::from_uri(&amqp_uri())
        .await
        .expect("failed to connect to RabbitMQ")
}
