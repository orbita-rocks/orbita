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

use std::sync::OnceLock;

use opentelemetry::metrics::{Gauge, Histogram};
use opentelemetry::{global, KeyValue};
use orbita_core::PartitionId;

/// The handles every signal records through, built once from the global meter.
struct Instruments {
    /// End-to-end request latency, in seconds, labelled by keyspace,
    /// partition, and operation. Whether a request was forwarded to the owner
    /// is left to the spans rather than a metric label: a route label would
    /// double every series, and the hop is exactly what a trace shows well.
    request_duration: Histogram<f64>,
    /// How many advertised replicas of a partition trail its committed prefix.
    replicas_behind: Gauge<u64>,
    /// The largest Lamport distance any replica of a partition trails its
    /// committed prefix by, which is the WAL replication lag.
    replication_lag: Gauge<u64>,
    /// A partition's on-disk size. Summed per keyspace it is that keyspace's
    /// quota consumption, which is why the keyspace label is carried here even
    /// though the number is measured per partition.
    partition_storage_bytes: Gauge<u64>,
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
            replicas_behind: meter
                .u64_gauge("orbita.wal.replicas_behind")
                .with_description(
                    "Advertised replicas of a partition that trail its committed prefix.",
                )
                .build(),
            replication_lag: meter
                .u64_gauge("orbita.wal.replication_lag")
                .with_unit("{lamport}")
                .with_description(
                    "Largest Lamport distance any replica of a partition trails its committed \
                     prefix by.",
                )
                .build(),
            partition_storage_bytes: meter
                .u64_gauge("orbita.partition.storage_bytes")
                .with_unit("By")
                .with_description(
                    "On-disk size of a partition. Summed per keyspace, this is quota consumption.",
                )
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

/// Records a partition's replication lag from its owner's point of view.
pub(crate) fn record_replication_lag(
    keyspace: &str,
    partition: PartitionId,
    lag: orbita_wal::ReplicationLag,
) {
    let attributes = [
        KeyValue::new("keyspace", keyspace.to_owned()),
        KeyValue::new("partition", partition.get() as i64),
    ];
    instruments()
        .replicas_behind
        .record(lag.replicas_behind, &attributes);
    instruments()
        .replication_lag
        .record(lag.max_lamports, &attributes);
}

/// Records a partition's storage footprint, which is per-keyspace quota
/// consumption once summed.
pub(crate) fn record_partition_storage(keyspace: &str, partition: PartitionId, bytes: u64) {
    instruments().partition_storage_bytes.record(
        bytes,
        &[
            KeyValue::new("keyspace", keyspace.to_owned()),
            KeyValue::new("partition", partition.get() as i64),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // With no meter provider installed the global meter is a no-op, so these
    // exercise that every instrument builds and every record path runs without
    // a provider behind it — which is exactly the state a single-node run or a
    // test is in. A panic here would mean a signal that only works once a
    // collector is configured, which is the opposite of the intent.
    #[test]
    fn every_signal_records_without_a_provider_installed() {
        let partition = PartitionId(7);
        record_request("tenant", partition, Operation::Get, 0.001);
        record_request("tenant", partition, Operation::Set, 0.002);
        record_request("tenant", partition, Operation::Delete, 0.003);
        record_request("tenant", partition, Operation::List, 0.004);
        record_replication_lag(
            "tenant",
            partition,
            orbita_wal::ReplicationLag {
                replicas_behind: 2,
                max_lamports: 9,
            },
        );
        record_partition_storage("tenant", partition, 4096);
    }
}
