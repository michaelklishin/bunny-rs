// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use proptest::prelude::*;

use bunny_rs::connection::topology::TopologyRegistry;
use bunny_rs::options::{ConsumeOptions, ExchangeDeclareOptions, QueueDeclareOptions};
use bunny_rs::protocol::types::FieldTable;

fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9.\\-]{0,30}"
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn prop_record_exchange_is_idempotent(name in arb_name()) {
        let mut reg = TopologyRegistry::default();
        let opts = ExchangeDeclareOptions::durable();
        reg.record_exchange(&name, "direct", &opts);
        reg.record_exchange(&name, "direct", &opts);
        let count = reg.exchanges.iter().filter(|e| e.name == name.as_str()).count();
        prop_assert_eq!(count, 1, "duplicate exchange recorded");
    }

    #[test]
    fn prop_record_queue_is_idempotent(name in arb_name()) {
        let mut reg = TopologyRegistry::default();
        let opts = QueueDeclareOptions::durable();
        reg.record_queue(&name, &opts, false);
        reg.record_queue(&name, &opts, false);
        let count = reg.queues.iter().filter(|q| q.name == name.as_str()).count();
        prop_assert_eq!(count, 1, "duplicate queue recorded");
    }

    #[test]
    fn prop_predeclared_exchanges_are_skipped(
        suffix in "[a-z.]{1,20}"
    ) {
        let mut reg = TopologyRegistry::default();
        let opts = ExchangeDeclareOptions::default();

        reg.record_exchange("", "direct", &opts);
        reg.record_exchange(&format!("amq.{suffix}"), "direct", &opts);

        prop_assert!(reg.exchanges.is_empty(), "predeclared exchange was recorded");
    }

    #[test]
    fn prop_user_exchanges_are_recorded(name in "[a-z][a-z0-9]{1,20}") {
        let mut reg = TopologyRegistry::default();
        let opts = ExchangeDeclareOptions::default();
        reg.record_exchange(&name, "fanout", &opts);
        prop_assert_eq!(reg.exchanges.len(), 1);
        prop_assert_eq!(reg.exchanges[0].name.as_str(), name.as_str());
    }

    #[test]
    fn prop_remove_queue_cascades_to_bindings_and_consumers(
        queue in arb_name(),
        exchange in arb_name(),
        tag in arb_name(),
    ) {
        let mut reg = TopologyRegistry::default();
        reg.record_queue(&queue, &QueueDeclareOptions::default(), false);
        reg.record_queue_binding(&queue, &exchange, "rk", &FieldTable::new());
        reg.record_consumer(1, &queue, &tag, &ConsumeOptions::default());

        prop_assert_eq!(reg.queues.len(), 1);
        prop_assert_eq!(reg.queue_bindings.len(), 1);
        prop_assert_eq!(reg.consumers.len(), 1);

        reg.remove_queue(&queue);

        prop_assert!(reg.queues.is_empty(), "queue not removed");
        prop_assert!(reg.queue_bindings.is_empty(), "binding not cascaded");
        prop_assert!(reg.consumers.is_empty(), "consumer not cascaded");
    }

    #[test]
    fn prop_remove_exchange_cascades_to_bindings(
        queue in arb_name(),
        exchange in arb_name(),
    ) {
        let mut reg = TopologyRegistry::default();
        let opts = ExchangeDeclareOptions::default();
        reg.record_exchange(&exchange, "direct", &opts);
        reg.record_queue_binding(&queue, &exchange, "rk", &FieldTable::new());

        reg.remove_exchange(&exchange);

        prop_assert!(reg.exchanges.is_empty(), "exchange not removed");
        prop_assert!(reg.queue_bindings.is_empty(), "binding not cascaded");
    }

    #[test]
    fn prop_remove_consumer_by_tag_is_precise(
        tag1 in arb_name(),
        tag2 in arb_name(),
    ) {
        prop_assume!(tag1 != tag2);

        let mut reg = TopologyRegistry::default();
        reg.record_consumer(1, "q", &tag1, &ConsumeOptions::default());
        reg.record_consumer(1, "q", &tag2, &ConsumeOptions::default());

        reg.remove_consumer(&tag1);

        prop_assert_eq!(reg.consumers.len(), 1);
        prop_assert_eq!(reg.consumers[0].consumer_tag.as_str(), tag2.as_str());
    }

    #[test]
    fn prop_clear_empties_everything(
        n_exchanges in 0..5usize,
        n_queues in 0..5usize,
        n_bindings in 0..5usize,
        n_consumers in 0..5usize,
    ) {
        let mut reg = TopologyRegistry::default();

        for i in 0..n_exchanges {
            reg.record_exchange(&format!("x-{i}"), "direct", &ExchangeDeclareOptions::default());
        }
        for i in 0..n_queues {
            reg.record_queue(&format!("q-{i}"), &QueueDeclareOptions::default(), false);
        }
        for i in 0..n_bindings {
            reg.record_queue_binding(&format!("q-{i}"), &format!("x-{i}"), "rk", &FieldTable::new());
        }
        for i in 0..n_consumers {
            reg.record_consumer(1, &format!("q-{i}"), &format!("c-{i}"), &ConsumeOptions::default());
        }

        reg.clear();

        prop_assert!(reg.exchanges.is_empty());
        prop_assert!(reg.queues.is_empty());
        prop_assert!(reg.queue_bindings.is_empty());
        prop_assert!(reg.consumers.is_empty());
    }

    #[test]
    fn prop_remove_then_rerecord_produces_one_entry(name in arb_name()) {
        let mut reg = TopologyRegistry::default();
        let opts = QueueDeclareOptions::default();

        reg.record_queue(&name, &opts, false);
        reg.remove_queue(&name);
        reg.record_queue(&name, &opts, false);

        let count = reg.queues.iter().filter(|q| q.name == name.as_str()).count();
        prop_assert_eq!(count, 1);
    }

    #[test]
    fn prop_remove_nonexistent_is_harmless(
        existing in arb_name(),
        missing in arb_name(),
    ) {
        prop_assume!(existing != missing);

        let mut reg = TopologyRegistry::default();
        reg.record_queue(&existing, &QueueDeclareOptions::default(), false);

        reg.remove_queue(&missing);
        reg.remove_exchange(&missing);
        reg.remove_consumer(&missing);

        prop_assert_eq!(reg.queues.len(), 1);
        prop_assert_eq!(reg.queues[0].name.as_str(), existing.as_str());
    }

    #[test]
    fn prop_remove_queue_does_not_affect_other_queues(
        q1 in arb_name(),
        q2 in arb_name(),
    ) {
        prop_assume!(q1 != q2);

        let mut reg = TopologyRegistry::default();
        reg.record_queue(&q1, &QueueDeclareOptions::default(), false);
        reg.record_queue(&q2, &QueueDeclareOptions::default(), false);
        reg.record_queue_binding(&q1, "x", "rk1", &FieldTable::new());
        reg.record_queue_binding(&q2, "x", "rk2", &FieldTable::new());

        reg.remove_queue(&q1);

        prop_assert_eq!(reg.queues.len(), 1);
        prop_assert_eq!(reg.queues[0].name.as_str(), q2.as_str());
        prop_assert_eq!(reg.queue_bindings.len(), 1);
        prop_assert_eq!(reg.queue_bindings[0].queue.as_str(), q2.as_str());
    }

    #[test]
    fn prop_update_queue_name_propagates(
        old_name in arb_name(),
        new_name in arb_name(),
        exchange in arb_name(),
        tag in arb_name(),
    ) {
        prop_assume!(old_name != new_name);

        let mut reg = TopologyRegistry::default();
        reg.record_queue(&old_name, &QueueDeclareOptions::default(), true);
        reg.record_queue_binding(&old_name, &exchange, "rk", &FieldTable::new());
        reg.record_consumer(1, &old_name, &tag, &ConsumeOptions::default());

        reg.update_queue_name(&old_name, &new_name);

        prop_assert_eq!(reg.queues[0].name.as_str(), new_name.as_str());
        prop_assert_eq!(reg.queue_bindings[0].queue.as_str(), new_name.as_str());
        prop_assert_eq!(reg.consumers[0].queue.as_str(), new_name.as_str());
    }

    #[test]
    fn prop_update_consumer_tag_propagates(
        old_tag in arb_name(),
        new_tag in arb_name(),
    ) {
        prop_assume!(old_tag != new_tag);

        let mut reg = TopologyRegistry::default();
        reg.record_consumer(1, "q", &old_tag, &ConsumeOptions::default());

        reg.update_consumer_tag(&old_tag, &new_tag);

        prop_assert_eq!(reg.consumers[0].consumer_tag.as_str(), new_tag.as_str());
    }

    #[test]
    fn prop_record_queue_binding_is_idempotent(
        queue in arb_name(),
        exchange in arb_name(),
        rk in arb_name(),
    ) {
        let mut reg = TopologyRegistry::default();
        let args = FieldTable::new();
        reg.record_queue_binding(&queue, &exchange, &rk, &args);
        reg.record_queue_binding(&queue, &exchange, &rk, &args);
        prop_assert_eq!(reg.queue_bindings.len(), 1, "duplicate queue binding recorded");
    }

    #[test]
    fn prop_record_exchange_binding_is_idempotent(
        dest in arb_name(),
        src in arb_name(),
        rk in arb_name(),
    ) {
        let mut reg = TopologyRegistry::default();
        let args = FieldTable::new();
        reg.record_exchange_binding(&dest, &src, &rk, &args);
        reg.record_exchange_binding(&dest, &src, &rk, &args);
        prop_assert_eq!(reg.exchange_bindings.len(), 1, "duplicate exchange binding recorded");
    }

    #[test]
    fn prop_remove_queue_binding_is_precise(
        queue in arb_name(),
        exchange in arb_name(),
        rk1 in arb_name(),
        rk2 in arb_name(),
    ) {
        prop_assume!(rk1 != rk2);

        let mut reg = TopologyRegistry::default();
        let args = FieldTable::new();
        reg.record_queue_binding(&queue, &exchange, &rk1, &args);
        reg.record_queue_binding(&queue, &exchange, &rk2, &args);

        reg.remove_queue_binding(&queue, &exchange, &rk1);

        prop_assert_eq!(reg.queue_bindings.len(), 1);
        prop_assert_eq!(reg.queue_bindings[0].routing_key.as_str(), rk2.as_str());
    }

    #[test]
    fn prop_remove_exchange_binding_is_precise(
        dest in arb_name(),
        src in arb_name(),
        rk1 in arb_name(),
        rk2 in arb_name(),
    ) {
        prop_assume!(rk1 != rk2);

        let mut reg = TopologyRegistry::default();
        let args = FieldTable::new();
        reg.record_exchange_binding(&dest, &src, &rk1, &args);
        reg.record_exchange_binding(&dest, &src, &rk2, &args);

        reg.remove_exchange_binding(&dest, &src, &rk1);

        prop_assert_eq!(reg.exchange_bindings.len(), 1);
        prop_assert_eq!(reg.exchange_bindings[0].routing_key.as_str(), rk2.as_str());
    }
}
