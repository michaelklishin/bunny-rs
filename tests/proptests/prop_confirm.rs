// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::collections::VecDeque;

use proptest::prelude::*;
use tokio::sync::oneshot;

use bunny_rs::channel::resolve_confirms;

/// Build a pending queue with sequence numbers `1..=n`, each with a fresh oneshot.
fn make_pending(
    n: u64,
) -> (
    VecDeque<(
        u64,
        oneshot::Sender<bool>,
        Option<tokio::sync::OwnedSemaphorePermit>,
    )>,
    Vec<oneshot::Receiver<bool>>,
) {
    let mut pending = VecDeque::new();
    let mut rxs = Vec::new();
    for seq in 1..=n {
        let (tx, rx) = oneshot::channel();
        pending.push_back((seq, tx, None));
        rxs.push(rx);
    }
    (pending, rxs)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    // Single-ack resolves exactly one entry and leaves the rest intact.
    #[test]
    fn prop_single_ack_resolves_one(n in 1u64..50, target in 1u64..50) {
        let n = n.max(target);
        let (mut pending, mut rxs) = make_pending(n);

        resolve_confirms(&mut pending, target, false, true);

        prop_assert_eq!(pending.len() as u64, n - 1, "one entry should be removed");
        prop_assert!(
            pending.iter().all(|(s, _, _)| *s != target),
            "resolved seq should be gone"
        );
        // The resolved receiver should yield `true`.
        let result = rxs[(target - 1) as usize].try_recv();
        prop_assert_eq!(result, Ok(true));
    }

    // Multiple-ack resolves all entries up to and including the tag.
    #[test]
    fn prop_multiple_ack_resolves_prefix(n in 1u64..50, tag in 1u64..50) {
        let n = n.max(tag);
        let (mut pending, mut rxs) = make_pending(n);

        resolve_confirms(&mut pending, tag, true, true);

        prop_assert_eq!(pending.len() as u64, n - tag);
        // All resolved receivers should yield `true`.
        for i in 0..tag as usize {
            prop_assert_eq!(rxs[i].try_recv(), Ok(true));
        }
        // Remaining receivers should still be open (no value yet).
        for i in tag as usize..n as usize {
            prop_assert!(rxs[i].try_recv().is_err(), "seq {} should still be pending", i + 1);
        }
    }

    // Nack (single) sends `false` to the resolved receiver.
    #[test]
    fn prop_single_nack_sends_false(n in 1u64..30, target in 1u64..30) {
        let n = n.max(target);
        let (mut pending, mut rxs) = make_pending(n);

        resolve_confirms(&mut pending, target, false, false);

        prop_assert_eq!(pending.len() as u64, n - 1);
        let result = rxs[(target - 1) as usize].try_recv();
        prop_assert_eq!(result, Ok(false));
    }

    // Multiple nack sends `false` to all resolved receivers.
    #[test]
    fn prop_multiple_nack_sends_false(n in 1u64..30, tag in 1u64..30) {
        let n = n.max(tag);
        let (mut pending, mut rxs) = make_pending(n);

        resolve_confirms(&mut pending, tag, true, false);

        for i in 0..tag as usize {
            prop_assert_eq!(rxs[i].try_recv(), Ok(false));
        }
    }

    // Resolving a tag that doesn't exist is a no-op.
    #[test]
    fn prop_missing_tag_is_noop(n in 1u64..20) {
        let (mut pending, _rxs) = make_pending(n);

        resolve_confirms(&mut pending, n + 100, false, true);
        prop_assert_eq!(pending.len() as u64, n);
    }

    // Resolving on an empty queue is a no-op.
    #[test]
    fn prop_empty_queue_is_noop(tag in 1u64..100, multiple in proptest::bool::ANY) {
        let mut pending = VecDeque::new();
        resolve_confirms(&mut pending, tag, multiple, true);
        prop_assert!(pending.is_empty());
    }

    // Two successive single acks resolve independently.
    #[test]
    fn prop_two_successive_acks(n in 3u64..30) {
        let (mut pending, mut rxs) = make_pending(n);

        resolve_confirms(&mut pending, 1, false, true);
        resolve_confirms(&mut pending, 2, false, true);

        prop_assert_eq!(pending.len() as u64, n - 2);
        prop_assert_eq!(rxs[0].try_recv(), Ok(true));
        prop_assert_eq!(rxs[1].try_recv(), Ok(true));
    }
}
