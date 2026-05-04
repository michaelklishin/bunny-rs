// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::collections::HashSet;

use bunny_rs::channel::generate_consumer_tag;

#[test]
fn generate_consumer_tag_format() {
    let tag = generate_consumer_tag();
    assert!(tag.starts_with("bunny-rs-"));
    let rest = &tag["bunny-rs-".len()..];
    let (ms, suffix) = rest.split_once('-').unwrap();
    assert!(ms.parse::<u64>().is_ok());
    assert!(suffix.parse::<u32>().is_ok());
}

// Birthday-paradox collision probability for 1000 draws from a 32-bit space
// is ~1.16e-4. A flake here is a real entropy regression.
#[test]
fn generate_consumer_tag_is_unique_under_load() {
    const N: usize = 1000;
    let mut seen = HashSet::with_capacity(N);
    for _ in 0..N {
        assert!(seen.insert(generate_consumer_tag()));
    }
}
