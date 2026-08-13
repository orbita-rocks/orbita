//! The node's OpenTelemetry metric instruments.
//!
//! Everything here reads from `opentelemetry::global`, which is a no-op meter
//! until the `orbita` binary installs an exporter. That is deliberate: a
//! library that emitted through its own provider would force every test and
//! every single-node run to stand up a metrics pipeline it never asked for.
//! The node process wires the exporter once (see `orbita_cli::telemetry`), and
//! until it does these calls cost an atomic load and return.
//!
//! # Cardinality
//!
//! Every series here is labelled by keyspace and, where it applies, by
//! partition. Both grow with the cluster and no further, which is the line the
//! observability requirement draws: per-partition is in, per-key is out. The
//! partition is emitted as an integer attribute rather than a formatted string
//! so a hot path does not allocate to describe itself, and no label ever
//! carries a key, a value, or a client identity — those would make the metric
//! grow with traffic rather than with the cluster.
//!
//! The instruments are built once, lazily, the first time a signal is emitted.
//! By then the node process has installed its meter provider, so the handles
//! bind to the real exporter rather than to the startup no-op.

use std::sync::{OnceLock, RwLock};

use opentelemetry::metrics::{Histogram, ObservableGauge};
use opentelemetry::{global, KeyValue};
use orbita_core::PartitionId;

/// One owned partition's latest gauge readings, as of the heartbeat that
/// published them.
///
/// `partition` is kept pre-widened to the `i64` the attribute wants so the
/// observable callback, which runs on the exporter's collection thread, does no
/// arithmetic and only clones the strings a `KeyValue` needs.
#[derive(Clone)]
struct OwnedSample {
    keyspace: String,
    partition: i64,
    replicas_behind: u64,
    replication_lag: u64,
    lease_holders: u64,
    /// `None` when the owner could not size the partition this heartbeat, in
    /// which case its storage series is left unreported rather than pinned at a
    /// stale or zero value.
    storage_bytes: Option<u64>,
}

/// The partitions this node currently owns, republished wholesale by every
/// heartbeat (see [`publish_owned`]).
///
/// The gauge callbacks read this and nothing else, so a partition that stops
/// being owned — retired, reassigned, or in a keyspace that was deleted — falls
/// out of the export the first heartbeat it is absent from this set, instead of
/// a synchronous last-value gauge freezing its final reading and exporting it
/// forever while quota sums and cardinality keep counting a partition that is
/// gone.
fn owned() -> &'static RwLock<Vec<OwnedSample>> {
    static OWNED: OnceLock<RwLock<Vec<OwnedSample>>> = OnceLock::new();
    OWNED.get_or_init(|| RwLock::new(Vec::new()))
}

/// The handles every signal records through, built once from the global meter.
///
/// The gauges are observable: they hold no per-partition state of their own and
/// instead read [`owned`] each collection, so retirement is expressed by a
/// partition leaving that set rather than by anyone remembering to zero a
/// series. The instrument handles are kept alive here only so their registered
/// callbacks are not dropped.
struct Instruments {
    /// End-to-end request latency, in seconds, labelled by keyspace,
    /// partition, and operation. Whether a request was forwarded to the owner
    /// is left to the spans rather than a metric label: a route label would
    /// double every series, and the hop is exactly what a trace shows well.
    request_duration: Histogram<f64>,
    _replicas_behind: ObservableGauge<u64>,
    _replication_lag: ObservableGauge<u64>,
    _lease_holders: ObservableGauge<u64>,
    _partition_storage_bytes: ObservableGauge<u64>,
}

/// The keyspace and partition labels for one owned sample.
fn owned_attributes(sample: &OwnedSample) -> [KeyValue; 2] {
    [
        KeyValue::new("keyspace", sample.keyspace.clone()),
        KeyValue::new("partition", sample.partition),
    ]
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("orbita-server");
        Instruments {
            request_duration: meter
                .f64_histogram("orbita.request.duration")
                .with_unit("s")
                .with_description("Client request latency, per keyspace, partition, and operation.")
                .build(),
            _replicas_behind: meter
                .u64_observable_gauge("orbita.wal.replicas_behind")
                .with_description(
                    "Advertised replicas of a partition that trail its committed prefix.",
                )
                .with_callback(|observer| {
                    for sample in owned().read().unwrap().iter() {
                        observer.observe(sample.replicas_behind, &owned_attributes(sample));
                    }
                })
                .build(),
            _replication_lag: meter
                .u64_observable_gauge("orbita.wal.replication_lag")
                .with_unit("{lamport}")
                .with_description(
                    "Largest Lamport distance any replica of a partition trails its committed \
                     prefix by.",
                )
                .with_callback(|observer| {
                    for sample in owned().read().unwrap().iter() {
                        observer.observe(sample.replication_lag, &owned_attributes(sample));
                    }
                })
                .build(),
            _lease_holders: meter
                .u64_observable_gauge("orbita.partition.lease_holders")
                .with_unit("{replica}")
                .with_description(
                    "Replicas holding a live read lease on a partition, which is the coherence \
                     quorum every write to it waits on.",
                )
                .with_callback(|observer| {
                    for sample in owned().read().unwrap().iter() {
                        observer.observe(sample.lease_holders, &owned_attributes(sample));
                    }
                })
                .build(),
            _partition_storage_bytes: meter
                .u64_observable_gauge("orbita.partition.storage_bytes")
                .with_unit("By")
                .with_description(
                    "On-disk size of a partition. Summed per keyspace, this is quota consumption.",
                )
                .with_callback(|observer| {
                    for sample in owned().read().unwrap().iter() {
                        if let Some(bytes) = sample.storage_bytes {
                            observer.observe(bytes, &owned_attributes(sample));
                        }
                    }
                })
                .build(),
        }
    })
}

/// Which operation a latency sample belongs to. A fixed set, so the label never
/// grows.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Operation {
    Get,
    Set,
    Delete,
    List,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Operation::Get => "get",
            Operation::Set => "set",
            Operation::Delete => "delete",
            Operation::List => "list",
        }
    }
}

/// Records one request's latency.
pub(crate) fn record_request(
    keyspace: &str,
    partition: PartitionId,
    operation: Operation,
    seconds: f64,
) {
    instruments().request_duration.record(
        seconds,
        &[
            KeyValue::new("keyspace", keyspace.to_owned()),
            KeyValue::new("partition", partition.get() as i64),
            KeyValue::new("operation", operation.as_str()),
        ],
    );
}

/// One owned partition's gauge readings for a single heartbeat.
pub(crate) struct OwnedMetric {
    /// The keyspace name the partition's series are grouped under.
    pub keyspace: String,
    /// The partition these readings belong to.
    pub partition: PartitionId,
    /// Its replication lag from the owner's point of view this heartbeat.
    pub replication_lag: orbita_wal::ReplicationLag,
    /// How many replicas held a live read lease when this was sampled, which is
    /// the coherence quorum a write on this partition waits for.
    pub lease_holders: u64,
    /// Its on-disk size, or `None` when the owner could not size it this
    /// heartbeat, so the storage series is left unreported rather than stale.
    pub storage_bytes: Option<u64>,
}

/// Republishes the full set of partitions this node owns.
///
/// Called wholesale each heartbeat rather than per partition, because the whole
/// point of the observable gauges is that the set they report is exactly the
/// set passed here: a partition that has been retired is expressed by its
/// absence from `owned`, and a synchronous per-partition record could never
/// express absence at all. Building the instruments here as well guarantees the
/// callbacks are registered against the installed provider even on a node that
/// has not yet served a client request.
pub(crate) fn publish_owned(owned_metrics: Vec<OwnedMetric>) {
    publish_owned_into(owned(), owned_metrics);
    instruments();
}

/// The replacement itself, against whichever set it is given.
///
/// Split from [`publish_owned`] so the retirement rule can be tested without
/// the process-global set. Every `Node` in the same test binary publishes into
/// that global through its lease heartbeat, so a test asserting on it is racing
/// every other test that owns a partition — which is exactly what made this
/// flake on CI while passing locally. See issue #152.
fn publish_owned_into(target: &RwLock<Vec<OwnedSample>>, owned_metrics: Vec<OwnedMetric>) {
    let samples = owned_metrics
        .into_iter()
        .map(|metric| OwnedSample {
            keyspace: metric.keyspace,
            partition: metric.partition.get() as i64,
            replicas_behind: metric.replication_lag.replicas_behind,
            replication_lag: metric.replication_lag.max_lamports,
            lease_holders: metric.lease_holders,
            storage_bytes: metric.storage_bytes,
        })
        .collect();
    // Wholesale, which is the whole mechanism: a partition retires by being
    // absent from the next publish rather than by anyone retiring its series.
    *target.write().unwrap() = samples;
}

#[cfg(test)]
mod tests {
    use super::*;

    // With no meter provider installed the global meter is a no-op, so this
    // exercises that the instruments build and the record path runs without a
    // provider behind it — which is exactly the state a single-node run or a
    // test is in. A panic here would mean a signal that only works once a
    // collector is configured, which is the opposite of the intent. The gauge
    // path is left to the retirement test below so the two do not race over the
    // shared owned set.
    #[test]
    fn request_latency_records_without_a_provider_installed() {
        let partition = PartitionId(7);
        record_request("tenant", partition, Operation::Get, 0.001);
        record_request("tenant", partition, Operation::Set, 0.002);
        record_request("tenant", partition, Operation::Delete, 0.003);
        record_request("tenant", partition, Operation::List, 0.004);
    }

    // The observable gauges report exactly the set `publish_owned` last handed
    // them, so a partition that stops being owned has to leave that set — this
    // is the mechanism that retires its series instead of exporting a frozen
    // last value forever.
    //
    // Against a set this test owns, not the process-global one. Every `Node` in
    // this binary publishes into the global through its lease heartbeat, so
    // asserting on it races every other test that owns a partition. That raced
    // quietly for a long time and only ever failed on CI. See issue #152.
    #[test]
    fn republishing_owned_metrics_retires_a_partition_that_is_no_longer_owned() {
        let lag = orbita_wal::ReplicationLag {
            replicas_behind: 0,
            max_lamports: 0,
        };
        let published: RwLock<Vec<OwnedSample>> = RwLock::new(Vec::new());
        let live = |set: &RwLock<Vec<OwnedSample>>| -> Vec<i64> {
            set.read().unwrap().iter().map(|s| s.partition).collect()
        };

        publish_owned_into(
            &published,
            vec![
                OwnedMetric {
                    keyspace: "tenant".to_owned(),
                    partition: PartitionId(7),
                    replication_lag: lag,
                    lease_holders: 2,
                    storage_bytes: Some(4096),
                },
                OwnedMetric {
                    keyspace: "tenant".to_owned(),
                    partition: PartitionId(8),
                    replication_lag: lag,
                    lease_holders: 2,
                    storage_bytes: None,
                },
            ],
        );
        assert_eq!(live(&published), vec![7, 8]);

        // A later heartbeat that no longer owns partition 7.
        publish_owned_into(
            &published,
            vec![OwnedMetric {
                keyspace: "tenant".to_owned(),
                partition: PartitionId(8),
                replication_lag: lag,
                lease_holders: 2,
                storage_bytes: None,
            }],
        );
        assert_eq!(live(&published), vec![8]);
    }
}
