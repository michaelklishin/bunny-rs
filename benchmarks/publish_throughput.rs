/// Publish+consume throughput benchmark.
/// Workloads match bunny-swift/bunny-zig for cross-client comparison.
use bunny_rs::*;
use std::time::Instant;
use tokio::task;

struct Workload {
    label: &'static str,
    body_size: usize,
    message_count: usize,
}

const WORKLOADS: &[Workload] = &[
    Workload {
        label: "100K x 12 B",
        body_size: 12,
        message_count: 100_000,
    },
    Workload {
        label: "500K x 12 B",
        body_size: 12,
        message_count: 500_000,
    },
    Workload {
        label: "100K x 128 B",
        body_size: 128,
        message_count: 100_000,
    },
    Workload {
        label: "100K x 1 KB",
        body_size: 1024,
        message_count: 100_000,
    },
    Workload {
        label: "500K x 1 KB",
        body_size: 1024,
        message_count: 500_000,
    },
];

const PREFETCH: u16 = 500;
const MULTI_ACK_EVERY: usize = 100;
const QUEUE_NAME: &str = "bunny-rs.bench";

const CONFIRM_QUEUE_NAME: &str = "bunny-rs.bench.confirms";
const CONFIRM_MESSAGES: usize = 100_000;
const CONFIRM_BODY_SIZE: usize = 100;
const CONFIRM_BATCH_SIZE: usize = 1024;

struct RunResult {
    label: &'static str,
    rate: f64,
    mb_sec: f64,
    ms: f64,
}

// -- Publish + consume (no confirms) ----------------------------------------

async fn run_workload(workload: &Workload) -> Result<RunResult, ConnectionError> {
    let payload = vec![0xABu8; workload.body_size];
    let message_count = workload.message_count;

    let mut pub_opts = ConnectionOptions::default();
    pub_opts.recovery.enabled = false;
    let pub_conn = Connection::open(pub_opts).await?;

    let mut con_opts = ConnectionOptions::default();
    con_opts.recovery.enabled = false;
    let con_conn = Connection::open(con_opts).await?;

    let mut pub_ch = pub_conn.open_channel().await?;
    let mut con_ch = con_conn.open_channel().await?;

    con_ch
        .queue_declare(QUEUE_NAME, QueueDeclareOptions::durable())
        .await?;
    con_ch.queue_purge(QUEUE_NAME).await?;

    let mut q = con_ch.queue(QUEUE_NAME);
    let mut consumer = q
        .subscribe(SubscribeOptions::manual_ack().prefetch(PREFETCH))
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let start = Instant::now();

    let pub_handle = task::spawn(async move {
        for _ in 0..message_count {
            pub_ch.publish("", QUEUE_NAME, &payload).await.ok();
        }
    });

    let mut received = 0usize;
    while received < message_count {
        if let Some(delivery) = consumer.recv().await {
            received += 1;
            if received.is_multiple_of(MULTI_ACK_EVERY) || received == message_count {
                delivery.ack_multiple().await.ok();
            }
        }
    }

    pub_handle.await.unwrap();

    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;
    let rate = message_count as f64 * 1000.0 / ms;
    let mb_sec = rate * workload.body_size as f64 / (1024.0 * 1024.0);

    con_ch
        .queue_delete(QUEUE_NAME, QueueDeleteOptions::default())
        .await?;
    pub_conn.close().await?;
    con_conn.close().await?;

    Ok(RunResult {
        label: workload.label,
        rate,
        mb_sec,
        ms,
    })
}

// -- Publisher confirms (publish-only, comparable to Ruby benchmark) ----------

async fn run_confirm_batch(label: &'static str) -> Result<RunResult, ConnectionError> {
    let payload = vec![0xABu8; CONFIRM_BODY_SIZE];
    let mut opts = ConnectionOptions::default();
    opts.recovery.enabled = false;
    let conn = Connection::open(opts).await?;
    let mut ch = conn.open_channel().await?;

    ch.confirm_select().await?;
    ch.queue_declare(CONFIRM_QUEUE_NAME, QueueDeclareOptions::default())
        .await?;
    ch.queue_purge(CONFIRM_QUEUE_NAME).await?;

    let start = Instant::now();

    let mut count = 0usize;
    for _ in 0..CONFIRM_MESSAGES {
        ch.basic_publish("", CONFIRM_QUEUE_NAME, &PublishOptions::default(), &payload)
            .await?;
        count += 1;
        if count >= CONFIRM_BATCH_SIZE {
            ch.wait_for_confirms().await?;
            count = 0;
        }
    }
    if count > 0 {
        ch.wait_for_confirms().await?;
    }

    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;
    let rate = CONFIRM_MESSAGES as f64 * 1000.0 / ms;
    let mb_sec = rate * CONFIRM_BODY_SIZE as f64 / (1024.0 * 1024.0);

    conn.close().await?;

    Ok(RunResult {
        label,
        rate,
        mb_sec,
        ms,
    })
}

async fn run_confirm_tracking(
    label: &'static str,
    outstanding_limit: usize,
) -> Result<RunResult, ConnectionError> {
    let payload = vec![0xABu8; CONFIRM_BODY_SIZE];
    let mut opts = ConnectionOptions::default();
    opts.recovery.enabled = false;
    let conn = Connection::open(opts).await?;
    let ch = conn.open_channel().await?;

    let mut ch = ch
        .into_confirm_mode_with_tracking(outstanding_limit)
        .await?;
    ch.queue_declare(CONFIRM_QUEUE_NAME, QueueDeclareOptions::default())
        .await?;
    ch.queue_purge(CONFIRM_QUEUE_NAME).await?;

    let start = Instant::now();

    for _ in 0..CONFIRM_MESSAGES {
        let _confirm = ch
            .publish("", CONFIRM_QUEUE_NAME, &PublishOptions::default(), &payload)
            .await?;
    }
    ch.wait_for_confirms().await?;

    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;
    let rate = CONFIRM_MESSAGES as f64 * 1000.0 / ms;
    let mb_sec = rate * CONFIRM_BODY_SIZE as f64 / (1024.0 * 1024.0);

    conn.close().await?;

    Ok(RunResult {
        label,
        rate,
        mb_sec,
        ms,
    })
}

#[tokio::main]
async fn main() -> Result<(), ConnectionError> {
    println!("bunny-rs benchmark");
    println!(
        "Prefetch: {}, multi-ack every {}",
        PREFETCH, MULTI_ACK_EVERY
    );
    println!("{}", "-".repeat(72));
    println!();
    println!("## publish + consume (no confirms)");
    println!();

    for workload in WORKLOADS {
        let r = run_workload(workload).await?;
        println!(
            "{:<18}  {:>8.0} msg/sec  {:>6.1} MB/sec  ({:.0} ms)",
            r.label, r.rate, r.mb_sec, r.ms
        );
    }

    println!();
    println!("{}", "-".repeat(72));
    println!();
    println!(
        "## publisher confirms ({} x {} B, batch {})",
        CONFIRM_MESSAGES, CONFIRM_BODY_SIZE, CONFIRM_BATCH_SIZE
    );
    println!();

    // Warmup
    let _ = run_confirm_batch("warmup").await;

    let batch = run_confirm_batch("batch wait_for_confirms").await?;
    println!(
        "{:<30}  {:>8.0} msg/sec  {:>6.1} MB/sec  ({:.0} ms)",
        batch.label, batch.rate, batch.mb_sec, batch.ms
    );

    let track_1024 = run_confirm_tracking("tracking (limit 1024)", 1024).await?;
    println!(
        "{:<30}  {:>8.0} msg/sec  {:>6.1} MB/sec  ({:.0} ms)",
        track_1024.label, track_1024.rate, track_1024.mb_sec, track_1024.ms
    );

    let track_512 = run_confirm_tracking("tracking (limit 512)", 512).await?;
    println!(
        "{:<30}  {:>8.0} msg/sec  {:>6.1} MB/sec  ({:.0} ms)",
        track_512.label, track_512.rate, track_512.mb_sec, track_512.ms
    );

    let track_256 = run_confirm_tracking("tracking (limit 256)", 256).await?;
    println!(
        "{:<30}  {:>8.0} msg/sec  {:>6.1} MB/sec  ({:.0} ms)",
        track_256.label, track_256.rate, track_256.mb_sec, track_256.ms
    );

    println!();
    println!("{}", "-".repeat(72));

    Ok(())
}
