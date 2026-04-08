// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

//! Stream-based [`Queue::subscribe`] consumer API.

use std::time::Duration;

use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::{Consumer, SubscribeOptions};
use tokio_stream::StreamExt;

use crate::test_helpers::connect;

async fn declare_purge(ch: &mut bunny_rs::Channel, name: &str) {
    ch.queue_declare(name, QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge(name).await.unwrap();
}

async fn drain_n(sub: &mut Consumer, n: usize) -> Vec<bunny_rs::Delivery> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("subscription stalled")
            .expect("subscription closed");
        out.push(d);
    }
    out
}

#[tokio::test]
async fn subscribe_receives_messages_with_manual_ack() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.manual";
    declare_purge(&mut ch, q).await;

    for i in 0..3u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut sub = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack())
        .await
        .unwrap();
    assert!(!sub.consumer_tag().is_empty());

    let deliveries = drain_n(&mut sub, 3).await;
    for (i, d) in deliveries.iter().enumerate() {
        assert_eq!(d.body.as_ref(), &[i as u8]);
        d.ack().await.unwrap();
    }

    sub.cancel().await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let leftover = ch.queue_purge(q).await.unwrap();
    assert_eq!(leftover, 0, "all messages should have been acked");

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_auto_ack_mode() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.auto";
    declare_purge(&mut ch, q).await;

    for i in 0..4u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut sub = ch
        .queue(q)
        .subscribe(SubscribeOptions::auto_ack())
        .await
        .unwrap();

    let deliveries = drain_n(&mut sub, 4).await;
    assert_eq!(deliveries.len(), 4);

    sub.cancel().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let leftover = ch.queue_purge(q).await.unwrap();
    assert_eq!(leftover, 0);

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_with_explicit_consumer_tag() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.tag";
    declare_purge(&mut ch, q).await;

    let sub = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("explicit-tag"))
        .await
        .unwrap();
    assert_eq!(sub.consumer_tag(), "explicit-tag");

    sub.cancel().await.unwrap();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_with_prefetch_shortcut() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.prefetch";
    declare_purge(&mut ch, q).await;

    for _ in 0..10u8 {
        ch.basic_publish("", q, &PublishOptions::default(), b"x")
            .await
            .unwrap();
    }

    let mut sub = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().prefetch(1))
        .await
        .unwrap();

    let mut acked = 0;
    for _ in 0..10 {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap()
            .unwrap();
        d.ack().await.unwrap();
        acked += 1;
    }
    assert_eq!(acked, 10);

    sub.cancel().await.unwrap();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// The subscription must not borrow the channel: we publish into the same
/// channel from inside the consume loop.
#[tokio::test]
async fn channel_can_publish_while_subscription_is_active() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let in_q = "bunny-rs.test.subscribe.echo-in";
    let out_q = "bunny-rs.test.subscribe.echo-out";
    declare_purge(&mut ch, in_q).await;
    declare_purge(&mut ch, out_q).await;

    for i in 0..3u8 {
        ch.basic_publish("", in_q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut sub = ch
        .queue(in_q)
        .subscribe(SubscribeOptions::manual_ack())
        .await
        .unwrap();

    // Echo each message into out_q from inside the loop, on the same channel
    // that the consumer was created on. This is the use case the Consumer
    // design exists for.
    for _ in 0..3 {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap()
            .unwrap();
        ch.basic_publish("", out_q, &PublishOptions::default(), &d.body)
            .await
            .unwrap();
        d.ack().await.unwrap();
    }
    sub.cancel().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..3 {
        let d = loop {
            if let Some(d) = ch.basic_get(out_q, false).await.unwrap() {
                break d;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        got.push(d.body[0]);
        d.ack().await.unwrap();
    }
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 2]);

    ch.queue_delete(in_q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.queue_delete(out_q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// `Consumer` implements `Stream`, so users can drive it with combinators.
#[tokio::test]
async fn consumer_works_via_stream_trait() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.stream-trait";
    declare_purge(&mut ch, q).await;

    for i in 0..3u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut sub = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack())
        .await
        .unwrap();

    let mut received = Vec::new();
    while received.len() < 3 {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .expect("stream closed early");
        received.push(d.body[0]);
        d.ack().await.unwrap();
    }
    received.sort_unstable();
    assert_eq!(received, vec![0, 1, 2]);

    sub.cancel().await.unwrap();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// Dropping a `Consumer` must cancel the broker-side consumer.
#[tokio::test]
async fn dropping_consumer_cancels_broker_consumer() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.drop-cancel";
    declare_purge(&mut ch, q).await;

    {
        let _sub = ch
            .queue(q)
            .subscribe(SubscribeOptions::manual_ack().consumer_tag("dropme"))
            .await
            .unwrap();
        let info = ch
            .queue_declare(q, QueueDeclareOptions::default())
            .await
            .unwrap();
        assert_eq!(info.consumer_count, 1);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let info = ch
        .queue_declare(q, QueueDeclareOptions::default())
        .await
        .unwrap();
    assert_eq!(info.consumer_count, 0);

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// After explicitly cancelling a subscription, the consumer tag must be free
/// to reuse on the same channel: pins the local cleanup path.
#[tokio::test]
async fn resubscribe_with_same_tag_after_explicit_cancel() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.resubscribe-same-tag";
    declare_purge(&mut ch, q).await;

    let sub = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("reuse-me"))
        .await
        .unwrap();
    sub.cancel().await.unwrap();

    // Reusing the tag would fail with a server-side duplicate-consumer-tag
    // error if the broker still tracked the old one.
    let mut sub2 = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("reuse-me"))
        .await
        .unwrap();
    assert_eq!(sub2.consumer_tag(), "reuse-me");

    ch.publish("", q, b"x").await.unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.body.as_ref(), b"x");
    d.ack().await.unwrap();

    sub2.cancel().await.unwrap();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// After dropping a `Consumer`, the channel must remain healthy and accept a
/// new consumer on the same queue. Exercises the opportunistic cleanup path
/// in `dispatch_delivery`.
#[tokio::test]
async fn opportunistic_cleanup_after_consumer_drop() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.drop-cleanup";
    declare_purge(&mut ch, q).await;

    {
        let _sub = ch
            .queue(q)
            .subscribe(SubscribeOptions::auto_ack().consumer_tag("drop-cleanup"))
            .await
            .unwrap();
    }

    // Anything still routed to the dropped tag is logged and reclaimed by
    // dispatch_delivery on the way through.
    for _ in 0..5 {
        ch.publish("", q, b"x").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    ch.queue_purge(q).await.unwrap();
    let mut sub2 = ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("drop-cleanup-fresh"))
        .await
        .unwrap();
    ch.publish("", q, b"y").await.unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.body.as_ref(), b"y");
    d.ack().await.unwrap();

    sub2.cancel().await.unwrap();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// Deleting the queue from a separate connection triggers a broker-initiated
/// `basic.cancel` for the active consumer. The subscriber's channel must
/// continue to process RPCs after the broker cancel arrives, and the
/// queue.delete RPC on the admin side must complete promptly.
#[tokio::test]
async fn subscription_survives_broker_initiated_cancel() {
    let sub_conn = connect().await;
    let admin_conn = connect().await;
    let mut sub_ch = sub_conn.open_channel().await.unwrap();
    let mut admin_ch = admin_conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe.broker-cancel";
    declare_purge(&mut admin_ch, q).await;

    let mut sub = sub_ch
        .queue(q)
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("broker-cancel"))
        .await
        .unwrap();

    admin_ch.publish("", q, b"before-delete").await.unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.body.as_ref(), b"before-delete");
    d.ack().await.unwrap();

    // Deleting the queue must complete promptly even though it triggers a
    // server-initiated basic.cancel for our consumer on sub_ch.
    tokio::time::timeout(
        Duration::from_secs(5),
        admin_ch.queue_delete(q, QueueDeleteOptions::default()),
    )
    .await
    .expect("queue.delete-ok did not arrive in time")
    .unwrap();

    // After the broker cancel propagates, sub_ch must still process RPCs.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let info = sub_ch
        .queue_declare(q, QueueDeclareOptions::default())
        .await
        .unwrap();
    assert_eq!(info.consumer_count, 0);

    drop(sub);
    sub_ch
        .queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    sub_ch.close().await.unwrap();
    admin_ch.close().await.unwrap();
    sub_conn.close().await.unwrap();
    admin_conn.close().await.unwrap();
}

/// `subscribe_with` (RPC handler) pattern: respond to each request from inside
/// the consume loop using the same channel for publish.
#[tokio::test]
async fn rpc_server_pattern_works() {
    let conn = connect().await;
    let mut server_ch = conn.open_channel().await.unwrap();
    let mut client_ch = conn.open_channel().await.unwrap();

    let req_q = "bunny-rs.test.subscribe.rpc-req";
    let reply_q = "bunny-rs.test.subscribe.rpc-reply";
    declare_purge(&mut server_ch, req_q).await;
    declare_purge(&mut client_ch, reply_q).await;

    // Server: subscribe to req_q, echo body back to reply_to
    let server_handle = tokio::spawn(async move {
        let mut sub = server_ch
            .queue(req_q)
            .subscribe(SubscribeOptions::manual_ack())
            .await
            .unwrap();
        let req = sub.recv().await.unwrap();
        let reply_to = req.reply_to().unwrap().to_string();
        let mut reply_props = PublishOptions::default();
        if let Some(corr) = req.correlation_id() {
            reply_props.properties = reply_props.properties.correlation_id(corr.to_string());
        }
        server_ch
            .basic_publish("", &reply_to, &reply_props, &req.body)
            .await
            .unwrap();
        req.ack().await.unwrap();
        sub.cancel().await.unwrap();
        server_ch.close().await.unwrap();
    });

    // Client: publish a request with reply_to + correlation_id
    let mut publish_opts = PublishOptions::default();
    publish_opts.properties = publish_opts
        .properties
        .reply_to(reply_q.to_string())
        .correlation_id("rpc-1");
    client_ch
        .basic_publish("", req_q, &publish_opts, b"ping")
        .await
        .unwrap();

    let mut reply_sub = client_ch
        .queue(reply_q)
        .subscribe(SubscribeOptions::auto_ack())
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), reply_sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.body.as_ref(), b"ping");
    assert_eq!(reply.correlation_id(), Some("rpc-1"));

    reply_sub.cancel().await.unwrap();
    server_handle.await.unwrap();

    client_ch
        .queue_delete(req_q, QueueDeleteOptions::default())
        .await
        .unwrap();
    client_ch
        .queue_delete(reply_q, QueueDeleteOptions::default())
        .await
        .unwrap();
    client_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
