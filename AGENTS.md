# Instructions for AI Agents

## What is This Codebase?

This is a Rust client for RabbitMQ with a typesafe pattern and an [automatic connection recovery](https://www.rabbitmq.com/client-libraries/java-api-guide) feature.
See [CONTRIBUTING.md](./CONTRIBUTING.md) for test running instructions and development setup.

## Build System

All the standard Cargo commands apply but with one important detail: make sure to add `--all-features` so that
optional features like `tls` and `zeroize` are included in builds, tests, and lints.

 * `cargo build --all-features` to build
 * `RUSTFLAGS="-D warnings" cargo nextest run --all-features` to run tests
 * `RUSTFLAGS="-D warnings" cargo clippy --all-features` to lint
 * `cargo fmt` to reformat
 * `cargo publish` to publish the crate

Always run `cargo check --all-features` before making changes to verify the codebase compiles cleanly.
If compilation fails, investigate and fix compilation errors before proceeding with any modifications.

## Key Files

 * `src/connection/mod.rs`: `Connection`, `ConnectionOptions`, `ConnectionEvent`, `TopologyRecoveryFilter`
 * `src/connection/endpoint.rs`: `Endpoint`, `AddressResolver` for endpoint lists and DNS expansion
 * `src/connection/handshake.rs`: AMQP 0-9-1 handshake and negotiation
 * `src/connection/recovery.rs`: automatic reconnection and topology replay
 * `src/connection/topology.rs`: `TopologyRegistry`, recorded entity types
 * `src/channel/mod.rs`: `Channel`, `ChannelEvent`, `AmqpReplyCode`, `ReturnedMessage`, `Delivery`, dispatcher
 * `src/confirm_channel.rs`: `ConfirmChannel` (publisher confirm mode wrapper)
 * `src/tx_channel.rs`: `TxChannel` (transaction mode wrapper)
 * `src/consumer.rs`: `Consumer` trait, `ConsumerHandle`
 * `src/options.rs`: queue/exchange/consume/publish option builders
 * `src/protocol/`: AMQP 0-9-1 codec, frame parsing, method definitions, field tables, properties
 * `src/transport/`: TCP and TLS transport

## Test Suite Layout

Tests are consolidated into two test binaries (plus the lib unit test binary) for faster compilation:

 * `tests/integration/`: integration tests that require a locally running RabbitMQ node. `test_helpers.rs` contains shared helper functions
 * `tests/proptests/`: property-based tests (`prop_*.rs` modules, no RabbitMQ needed)

Each directory has a `main.rs` crate root that declares all modules.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for step-by-step Docker setup instructions for the local test environment.

### `nextest` Test Filters

Key [`nextest` filterset predicates](https://nexte.st/docs/filtersets/reference/):

 * `test(pattern)`: matches test names using a substring (e.g. `test(list_nodes)`)
 * `binary(pattern)`: matches the test binary name (e.g. `binary(integration)`, `binary(unit)`, `binary(proptests)`)
 * `package(name)`: matches by package (e.g. `package(bunny-rs)`)
 * `test(=exact_name)`: an exact test name match
 * `test(/regex/)`: like `test(pattern)` but uses regular expression matching
 * Set operations: `expr1 + expr2` (union), `expr1 - expr2` (difference), `not expr` (negation)

To run all tests in a specific module, use `cargo nextest run --all-features -E 'test(queue_tests::)'`.
To run only property-based tests (no RabbitMQ needed): `cargo nextest run --all-features -E 'binary(proptests)'`.

### Property-based Tests

Property-based tests are written using [proptest](https://docs.rs/proptest/latest/proptest/) and
use a naming convention: they begin with `prop_`.

To run the property-based tests specifically, use `cargo nextest run --all-features -E 'binary(proptests)'`.

## Source of Domain Knowledge

 * [RabbitMQ Java client](https://www.rabbitmq.com/client-libraries/java-api-guide)
 * [RabbitMQ .NET client](https://www.rabbitmq.com/client-libraries/dotnet-api-guide)
 * [Ruby Bunny](https://github.com/ruby-amqp/bunny)
 * [Swift Bunny](https://github.com/michaelklishin/bunny-swift)
 * [RabbitMQ Documentation](https://www.rabbitmq.com/docs/)

Treat this documentation as the ultimate first party source of truth.

## Change Log

If asked to perform change log updates, consult and modify `CHANGELOG.md` and stick to its
existing writing style.

## Releases

### How to Roll (Produce) a New Release

Suppose the current development version in `Cargo.toml` is `0.N.0` and `CHANGELOG.md` has
a `## v0.N.0 (in development)` section at the top.

To produce a new release:

 1. Update the changelog: replace `(in development)` with today's date, e.g. `(Feb 20, 2026)`. Make sure all notable changes since the previous release are listed
 2. Commit with the message `0.N.0` (just the version number, nothing else)
 3. Tag the commit: `git tag v0.N.0`
 4. Bump the dev version: back on `main`, set `Cargo.toml` version to `0.(N+1).0`
 5. Add a new `## v0.(N+1).0 (in development)` section to `CHANGELOG.md` with `No changes yet.` underneath
 6. Commit with the message `Bump dev version`
 7. Push: `git push && git push --tags`

The tag push triggers `.github/workflows/release.yml`, which publishes the crate to crates.io
via Trusted Publishing (OIDC) and creates a GitHub Release with changelog notes. No manual
`cargo publish` needed.

### GitHub Actions

The release workflow uses [`michaelklishin/rust-build-package-release-action`](https://github.com/michaelklishin/rust-build-package-release-action).

For verifying YAML file syntax, use `yq`, Ruby or Python YAML modules (whichever is available).

## Git Commits

 * Do not commit changes automatically without an explicit permission to do so
 * Never add yourself as a git commit coauthor
 * Never mention yourself in commit messages in any way (no "Generated by", no AI tool links, etc)
