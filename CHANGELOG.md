# Change Log

## v0.9.0 (in development)

Initial public release.

### Features

 * A port of Ruby and Swift implementations of Bunny, with Tokio and `async`/`await`
 * Automatic connection recovery with topology replay and endpoint shuffling
 * Streaming publisher confirms via `ConfirmChannel`
 * Consumer with async delivery stream
 * Type-safe builders for quorum queues, streams, delayed queues, JMS queues, dead-lettering, TTL, and more
 * TLS with `rustls` and platform-native certificate verification, support for the [`EXTERNAL` authN mechanism](https://www.rabbitmq.com/docs/ssl#trust-store-x509-auth)
 * Mutual peer certificate chain verification (mTLS) support
 * Optional `zeroize` support
 * Forward-compatible with RabbitMQ 4.3+ (deprecated features disabled by default are not exposed in the API)
