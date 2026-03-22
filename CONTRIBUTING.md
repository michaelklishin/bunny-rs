# Contributing

See also [AGENTS.md](./AGENTS.md) for a high-level codebase overview and conventions.

## Running Tests

Most tests require a locally running RabbitMQ node. The easiest way to get one is via Docker.

### Prerequisites

Install [cargo-nextest](https://nexte.st/) if you don't have it:

```bash
cargo install cargo-nextest
```

### Step 1: Start RabbitMQ

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  rabbitmq:4.1
```

Wait for the node to boot:

```bash
sleep 15
```

### Step 2: Run All Tests

```bash
RUSTFLAGS="-D warnings" cargo nextest run --all-features
```

### Stopping the Node

```bash
docker stop rabbitmq && docker rm rabbitmq
```

---

## `nextest` Test Filters

Key [`nextest` filterset predicates](https://nexte.st/docs/filtersets/reference/):

* `test(pattern)`: matches test names using a substring (e.g. `test(queue_tests)`)
* `binary(pattern)`: matches the test binary name (e.g. `binary(integration)`, `binary(proptests)`)
* `package(name)`: matches by package (e.g. `package(bunny-rs)`)
* `test(=exact_name)`: an exact test name match
* `test(/regex/)`: like `test(pattern)` but uses regular expression matching
* Set operations: `expr1 + expr2` (union), `expr1 - expr2` (difference), `not expr` (negation)

### Run All Tests in a Specific Module

```bash
cargo nextest run --all-features -E 'test(queue_tests::)'
```

### Run Only Property-based Tests (no RabbitMQ needed)

```bash
cargo nextest run --all-features -E 'binary(proptests)'
```

### Run a Specific Test by Name

```bash
cargo nextest run --all-features -E 'test(=test_publish_and_consume)'
```

---

## Running TLS Integration Tests

TLS tests require certificate and key pairs plus a TLS-configured RabbitMQ node.
They are excluded from standard `cargo nextest` runs.

Generate certificates using [tls-gen](https://github.com/rabbitmq/tls-gen):

```shell
cd /path/to/tls-gen/basic && make CN=localhost && make alias-leaf-artifacts
```

Then [configure](https://www.rabbitmq.com/docs/ssl) RabbitMQ to use TLS.

Export the `TLS_CERTS_DIR` environment variable:

```shell
export TLS_CERTS_DIR=/path/to/tls-gen/basic/result
```

Run the TLS tests:

```shell
cargo nextest run --all-features --run-ignored=only -E 'test(tls_tests::)'
```
