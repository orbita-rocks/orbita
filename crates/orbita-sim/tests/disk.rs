//! What the simulated disk promises, and what it refuses to promise.

use orbita_core::NodeId;
use orbita_runtime::{Disk, DiskError, File, OpenOptions, Runtime};
use orbita_sim::{DiskPolicy, SimConfig, Simulation};

use bytes::Bytes;

/// Turns one fault on for certain and leaves the rest off, so a test observes
/// the fault it names rather than whichever one fired first.
fn only(seed: u64, set: impl FnOnce(&mut orbita_sim::DiskFaults)) -> SimConfig {
    let mut config = SimConfig::new(seed);
    set(&mut config.disk);
    config
}

#[test]
fn appends_read_back_at_the_offset_they_reported() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    sim.block_on(async move {
        let file = disk.open("wal/1.log", OpenOptions::create()).await.unwrap();
        let first = file.append(Bytes::from_static(b"hello ")).await.unwrap();
        let second = file.append(Bytes::from_static(b"world")).await.unwrap();
        file.sync().await.unwrap();

        assert_eq!(first, 0);
        assert_eq!(second, 6);
        assert_eq!(
            file.read_at(0, 11).await.unwrap(),
            Bytes::from_static(b"hello world")
        );
        assert_eq!(file.size().await.unwrap(), 11);
        assert!(matches!(
            file.read_at(6, 20).await,
            Err(DiskError::OutOfBounds { .. })
        ));
    });
}

#[test]
fn listing_is_ordered_and_scoped_to_the_prefix() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    let names = sim.block_on(async move {
        for path in ["wal/2.log", "wal/1.log", "sst/a.sst"] {
            disk.open(path, OpenOptions::create()).await.unwrap();
        }
        disk.list("wal/").await.unwrap()
    });

    assert_eq!(
        names,
        vec!["wal/1.log".to_string(), "wal/2.log".to_string()]
    );
}

#[test]
fn opening_a_missing_file_without_create_is_distinguishable_from_an_io_error() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    sim.block_on(async move {
        assert!(matches!(
            disk.open("nope.log", OpenOptions::default()).await,
            Err(DiskError::NotFound(_))
        ));
    });
}

#[test]
fn a_crash_loses_everything_that_was_never_synced() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        file.append(Bytes::from_static(b"durable")).await.unwrap();
        file.sync().await.unwrap();
        file.append(Bytes::from_static(b"volatile")).await.unwrap();
    });

    sim.crash(NodeId(1));
    let node = sim.restart(NodeId(1), DiskPolicy::Intact);
    let disk = node.disk().clone();

    let survived = sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        let len = file.size().await.unwrap() as usize;
        file.read_at(0, len).await.unwrap()
    });

    assert_eq!(survived, Bytes::from_static(b"durable"));
}

#[test]
fn an_fsync_that_lies_loses_data_the_caller_was_told_was_durable() {
    let config = only(1, |disk| disk.lying_fsync_permille = 1000);
    let sim = Simulation::with_config(config);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        file.append(Bytes::from_static(b"acknowledged"))
            .await
            .unwrap();
        // The caller has no way to tell this apart from a real sync, which is
        // the entire point of simulating it.
        file.sync().await.unwrap();
    });

    sim.crash(NodeId(1));
    let node = sim.restart(NodeId(1), DiskPolicy::Intact);
    let disk = node.disk().clone();

    let size = sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        file.size().await.unwrap()
    });

    assert_eq!(
        size, 0,
        "a lying fsync must lose the write, or the fault is not being injected"
    );
}

#[test]
fn a_partial_write_leaves_a_prefix_behind_and_reports_failure() {
    let config = only(1, |disk| disk.partial_write_permille = 1000);
    let sim = Simulation::with_config(config);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    let (result, size) = sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        let result = file.append(Bytes::from_static(b"0123456789")).await;
        let size = file.size().await.unwrap();
        (result, size)
    });

    assert!(matches!(result, Err(DiskError::Io(_))));
    assert!(
        (1..10).contains(&size),
        "a partial write wrote {size} of 10 bytes, which is not partial"
    );
}

#[test]
fn a_failed_write_writes_nothing() {
    let config = only(1, |disk| disk.write_failure_permille = 1000);
    let sim = Simulation::with_config(config);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    let (result, size) = sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        let result = file.append(Bytes::from_static(b"0123456789")).await;
        let size = file.size().await.unwrap();
        (result, size)
    });

    assert!(matches!(result, Err(DiskError::Io(_))));
    assert_eq!(size, 0);
}

#[test]
fn corruption_is_reported_at_the_offset_it_was_found() {
    let config = only(1, |disk| disk.read_corruption_permille = 1000);
    let sim = Simulation::with_config(config);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        file.append(Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        assert_eq!(
            file.read_at(4, 2).await,
            Err(DiskError::Corrupt { offset: 4 })
        );
    });
}

#[test]
fn a_torn_tail_keeps_part_of_the_last_record() {
    // Seeds vary in how much of the tail survives, so this asserts the shape
    // rather than a number, and asserts across enough seeds that a torn tail
    // must have happened at least once.
    let mut saw_torn = false;
    for seed in 1..=20 {
        let mut config = SimConfig::new(seed);
        config.disk.torn_tail_on_crash = true;
        let sim = Simulation::with_config(config);
        let node = sim.add_node(NodeId(1));
        let disk = node.disk().clone();

        sim.block_on(async move {
            let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
            file.append(Bytes::from_static(b"committed")).await.unwrap();
            file.sync().await.unwrap();
            file.append(Bytes::from_static(b"half-written record"))
                .await
                .unwrap();
        });

        sim.crash(NodeId(1));
        let node = sim.restart(NodeId(1), DiskPolicy::Intact);
        let disk = node.disk().clone();
        let size = sim.block_on(async move {
            let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
            file.size().await.unwrap()
        });

        assert!(
            (9..9 + 19).contains(&size),
            "seed {seed}: recovery found {size} bytes, which is neither the committed \
             prefix nor a torn tail"
        );
        if size > 9 {
            saw_torn = true;
        }
    }
    assert!(saw_torn, "no seed produced a torn tail at all");
}

#[test]
fn a_node_that_comes_back_without_its_disk_finds_nothing() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let disk = node.disk().clone();

    let stale = sim.block_on(async move {
        let file = disk.open("wal.log", OpenOptions::create()).await.unwrap();
        file.append(Bytes::from_static(b"gone")).await.unwrap();
        file.sync().await.unwrap();
        file
    });

    sim.crash(NodeId(1));
    let node = sim.restart(NodeId(1), DiskPolicy::Lost);
    let disk = node.disk().clone();

    let names = sim.block_on(async move { disk.list("").await.unwrap() });
    assert!(names.is_empty(), "a replaced machine kept its old files");

    // A handle held across the replacement must fail rather than quietly
    // addressing whatever now lives at that path.
    let after = sim.block_on(async move { stale.size().await });
    assert!(matches!(after, Err(DiskError::Io(_))));
}
