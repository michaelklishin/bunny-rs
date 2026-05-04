# Change Log

## v0.11.0 (in development)

### Enhancements

 * `Delivery` gained `nack_discard` and `nack_discard_multiple`: the `requeue = false`
   counterparts to `nack` and `nack_multiple`. Combined with the existing `ack`, `ack_multiple`,
   `nack`, `nack_multiple`, `reject`, and `discard`, this provides a high level
   [consumer acknowledgement](https://www.rabbitmq.com/docs/confirms) API right on
   the `Delivery` struct

 * `Channel` gained matching `nack`, `nack_discard`, and `nack_discard_multiple` shortcuts


## v0.10.0 (Mar 26, 2026)

### Enhancements

 * `channel::Channel` [acknowledgement](https://rabbitmq.com/docs/confirms) helpers: `ack`, `ack_multiple`, `reject`, `discard`, `nack_multiple`
   for the common single-delivery and batch cases

 * `PublishOptions::mandatory` is a new helper constructor with a [self-describing name](https://www.rabbitmq.com/docs/publishers#unroutable)

 * Adopt Trusted Publishing (OIDC) for publishing to `crates.io`



## v0.9.0 (Mar 26, 2026)

Initial public release.

### Features

 * A port of Ruby and Swift implementations of Bunny, with Tokio and `async`/`await`
 * Automatic connection recovery with topology replay and endpoint shuffling
 * Streaming publisher confirms via `ConfirmChannel`
 * Consumer with async delivery stream
 * Type-safe builders for quorum queues, streams, delayed queues, JMS queues, dead-lettering, TTL, and more
 * TLS with `rustls` and platform-native certificate verification, support for the [`EXTERNAL` authN mechanism](https://www.rabbitmq.com/docs/ssl#trust-store-x509-auth)
 * Mutual [peer certificate chain verification](https://www.rabbitmq.com/docs/ssl#peer-verification) (mTLS) support
 * Optional `zeroize` support
 * Forward-compatible with RabbitMQ 4.3+ (deprecated features disabled by default are not exposed in the API)
