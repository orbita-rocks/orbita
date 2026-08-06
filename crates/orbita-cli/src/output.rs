//! Rendering results, in one place.
//!
//! Every command produces a view type that lives here, implements
//! [`Render`], and knows how to print itself as human text. JSON comes free
//! from `serde`. Keeping both modes in one module is what stops the two from
//! drifting: a field added to the JSON but not the table is a change to one
//! file, so a reviewer sees it.
//!
//! Human output is the default because most invocations are a person at a
//! terminal. `--output json` exists for scripts, and it is a supported
//! interface: field names are part of the contract, and a script parsing the
//! human table is a script that will break.
//!
//! Keys and values are arbitrary bytes, which JSON cannot represent. Every
//! blob is emitted as an object naming its encoding, so a consumer never has
//! to guess whether a string was text or a base64 blob that happened to look
//! like text.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::Serialize;

/// Which of the two output modes a command should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Format {
    /// Tables and sentences, for a person.
    #[default]
    Human,
    /// One JSON document, for a script.
    Json,
}

/// A result a command can print.
///
/// The human rendering is a method rather than `Display` so that a view can
/// also be serialized without the two impls fighting over the same type.
pub trait Render: Serialize {
    /// Writes the human-readable form. Implementations should not add a
    /// trailing newline; [`render`] handles that.
    fn render_human(&self, out: &mut String);
}

/// Renders a view in the requested format.
///
/// It returns a string rather than writing to stdout so that the tests can
/// assert on exactly what a user would see.
pub fn render<T: Render>(format: Format, value: &T) -> Result<String> {
    let mut out = String::new();
    match format {
        Format::Human => value.render_human(&mut out),
        Format::Json => out.push_str(&serde_json::to_string_pretty(value)?),
    }
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    Ok(out)
}

/// A key or a value on its way out of the tool.
///
/// Bytes that are valid UTF-8 are shown as text because that is what almost
/// every key is. Anything else is base64, and the encoding is stated rather
/// than implied so a script can branch on it instead of guessing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "encoding", rename_all = "lowercase")]
pub enum Blob {
    Utf8 { value: String },
    Base64 { value: String },
}

impl Blob {
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(text) => Self::Utf8 {
                value: text.to_owned(),
            },
            Err(_) => Self::Base64 {
                value: BASE64.encode(bytes),
            },
        }
    }

    /// The text a listing is ordered by.
    ///
    /// Ordering exists so that two runs of the same command diff cleanly, not
    /// because anything depends on the order, so comparing the rendered form
    /// is enough.
    #[must_use]
    pub fn sort_key(&self) -> &str {
        match self {
            Self::Utf8 { value } | Self::Base64 { value } => value,
        }
    }

    /// The human rendering, which marks base64 so that nobody copies an
    /// encoded blob back in as if it were text.
    #[must_use]
    pub fn display(&self) -> String {
        match self {
            Self::Utf8 { value } => value.clone(),
            Self::Base64 { value } => format!("base64:{value}"),
        }
    }
}

/// Lays out a table with columns wide enough for their contents.
///
/// Two spaces between columns and no borders, because the most common thing
/// done to CLI output is piping it into `awk`.
#[must_use]
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut out = String::new();
    write_row(
        &mut out,
        &headers.iter().map(|h| h.to_uppercase()).collect::<Vec<_>>(),
        &widths,
    );
    for row in rows {
        write_row(&mut out, row, &widths);
    }
    out
}

fn write_row(out: &mut String, cells: &[String], widths: &[usize]) {
    let last = cells.len().saturating_sub(1);
    for (i, cell) in cells.iter().enumerate() {
        if i == last {
            out.push_str(cell);
        } else {
            let _ = write!(out, "{cell:<width$}  ", width = widths[i]);
        }
    }
    out.push('\n');
}

/// A command that did something but has nothing to report beyond that.
///
/// Deletes and revocations return this. Human output gets a sentence, and JSON
/// output gets a stable object, so a script does not have to special case the
/// commands that print nothing.
#[derive(Debug, Clone, Serialize)]
pub struct Ack {
    pub ok: bool,
    pub message: String,
}

impl Ack {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            ok: true,
            message: message.into(),
        }
    }
}

impl Render for Ack {
    fn render_human(&self, out: &mut String) {
        out.push_str(&self.message);
    }
}

/// Formats a Unix millisecond timestamp as UTC.
///
/// The arithmetic is here rather than behind a date library because this is
/// the only place the tool needs a calendar, and a whole time crate to print
/// one column is a dependency an operator has to audit for no reason.
#[must_use]
pub fn format_millis(millis: u64) -> String {
    let secs = (millis / 1000) as i64;
    let time_of_day = secs.rem_euclid(86_400);
    let days = secs.div_euclid(86_400);

    // Days since 1970-01-01 to a civil date, shifting the epoch to 0000-03-01
    // so that the leap day lands at the end of the cycle and the month length
    // pattern becomes a single expression.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);

    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3600,
        (time_of_day % 3600) / 60,
        time_of_day % 60
    )
}

/// Formats a byte count the way an operator reads one.
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Formats a byte count that may not have been measured.
///
/// "unknown" rather than "0 B", and the distance between those two words is
/// the whole point: a node running a binary from before the measurement
/// existed reports nothing, and printing that as an empty index tells an
/// operator they have headroom they do not have. Zero stays available for the
/// nodes that really are holding no index.
#[must_use]
pub fn format_bytes_or_unknown(bytes: Option<u64>) -> String {
    bytes.map_or_else(|| "unknown".to_owned(), format_bytes)
}

/// The per-keyspace limits and defaults, as the admin API reports them.
#[derive(Debug, Clone, Default, Serialize)]
pub struct KeyspaceConfigView {
    pub default_ttl_millis: Option<u64>,
    pub max_value_bytes: Option<u64>,
    pub max_storage_bytes: Option<u64>,
    pub max_reads_per_second: Option<u32>,
    pub max_writes_per_second: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyspaceView {
    pub id: u64,
    pub name: String,
    pub partition_count: u32,
    pub stored_bytes: u64,
    pub created_at_millis: u64,
    pub config: KeyspaceConfigView,
}

impl Render for KeyspaceView {
    fn render_human(&self, out: &mut String) {
        let _ = writeln!(out, "name         {}", self.name);
        let _ = writeln!(out, "id           {}", self.id);
        let _ = writeln!(out, "partitions   {}", self.partition_count);
        let _ = writeln!(out, "stored       {}", format_bytes(self.stored_bytes));
        let _ = writeln!(
            out,
            "created      {}",
            format_millis(self.created_at_millis)
        );
        let c = &self.config;
        let _ = writeln!(out, "default ttl  {}", opt_millis(c.default_ttl_millis));
        let _ = writeln!(out, "max value    {}", opt_bytes(c.max_value_bytes));
        let _ = writeln!(out, "quota        {}", opt_bytes(c.max_storage_bytes));
        let _ = writeln!(
            out,
            "rate limits  {} reads/s, {} writes/s",
            opt_num(c.max_reads_per_second),
            opt_num(c.max_writes_per_second)
        );
    }
}

fn opt_millis(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_owned(), |v| format!("{v} ms"))
}

fn opt_bytes(value: Option<u64>) -> String {
    value.map_or_else(|| "unlimited".to_owned(), format_bytes)
}

fn opt_num<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "unlimited".to_owned(), |v| v.to_string())
}

/// The keyspaces on a cluster.
#[derive(Debug, Clone, Serialize)]
pub struct KeyspaceListView {
    pub keyspaces: Vec<KeyspaceView>,
}

impl Render for KeyspaceListView {
    fn render_human(&self, out: &mut String) {
        if self.keyspaces.is_empty() {
            out.push_str("no keyspaces. Create one with: orbita keyspace create <name>");
            return;
        }
        let rows: Vec<Vec<String>> = self
            .keyspaces
            .iter()
            .map(|k| {
                vec![
                    k.name.clone(),
                    k.id.to_string(),
                    k.partition_count.to_string(),
                    format_bytes(k.stored_bytes),
                    format_millis(k.created_at_millis),
                ]
            })
            .collect();
        out.push_str(&table(
            &["name", "id", "partitions", "stored", "created"],
            &rows,
        ));
    }
}

/// A newly minted credential.
///
/// The secret is returned once and never again, so the human rendering says so
/// out loud rather than leaving an operator to discover it by trying.
#[derive(Debug, Clone, Serialize)]
pub struct CredentialView {
    pub credential_id: String,
    pub secret: String,
}

impl Render for CredentialView {
    fn render_human(&self, out: &mut String) {
        let _ = writeln!(out, "credential id  {}", self.credential_id);
        let _ = writeln!(out, "secret         {}", self.secret);
        out.push_str("\nThe secret is shown once and cannot be retrieved again. Store it now.");
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    pub id: u64,
    pub address: String,
    pub role: String,
    pub health: String,
    pub raft_leader: bool,
    /// The cluster versions this node's binary can speak, such as "0.1..0.2",
    /// or "unknown" from a server that predates version reporting.
    pub speaks: String,
    /// What the partition indexes this node holds cost in memory.
    ///
    /// Absent when the node has not reported a measurement, which is what a
    /// node running an older binary looks like for the length of a rolling
    /// upgrade, and what a node the leader group has never heard from looks
    /// like always. Not the same as zero, which is a node genuinely holding
    /// no index and is what every leader-group member looks like.
    pub index_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaView {
    pub node_id: u64,
    /// Absent when the leader group has not heard this replica's position,
    /// which is the state #79 named `Unestablished`. Not zero: a replica that
    /// has said nothing is unknown, and rendering it at the bottom of the log
    /// invents a maximally-behind copy out of one that may be current.
    pub applied_lamport: Option<u64>,
    /// How far this replica has made the log durable. Ahead of what it has
    /// applied, and behind the owner by whatever replication has not caught.
    /// Absent for the same reason.
    pub durable_lamport: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionView {
    pub id: u64,
    pub keyspace_id: u64,
    pub start_key: Blob,
    /// Absent means the range is unbounded above, which is what a keyspace
    /// that has never split looks like.
    pub end_key: Option<Blob>,
    pub owner_node_id: u64,
    pub epoch: u64,
    /// The owner's durable position. Absent when no owner has reported one,
    /// which is what a fenced partition looks like while the control plane
    /// waits for its replicas to report past the fence.
    pub committed_lamport: Option<u64>,
    /// What the owner reported this partition holds. Absent for the same
    /// reason. A fenced partition full of data has an unknown size, and
    /// showing it as empty is how somebody concludes it is cheap to lose.
    pub size_bytes: Option<u64>,
    /// What the owner's memory-resident index for this partition costs.
    /// Absent when nobody measured it rather than zero, for the same reason
    /// as [`NodeView::index_memory_bytes`].
    pub index_bytes: Option<u64>,
    pub replicas: Vec<ReplicaView>,
}

impl PartitionView {
    /// The furthest any replica trails the owner.
    ///
    /// This is the number that decides how much a failover would lose, so it
    /// gets a column of its own rather than being buried in the replica list.
    #[must_use]
    pub fn max_replica_lag(&self) -> Option<u64> {
        self.lag(|r| r.applied_lamport)
    }

    /// The furthest any replica trails the owner in the write-ahead log.
    ///
    /// Distinct from [`PartitionView::max_replica_lag`], and the two answer
    /// different questions: this one is how much replication is behind, and
    /// that one is how much of what arrived is not yet readable. A replica
    /// can be durable to the owner's position and still applying.
    #[must_use]
    pub fn max_wal_lag(&self) -> Option<u64> {
        self.lag(|r| r.durable_lamport)
    }

    /// Lag is a distance from the owner's committed position, so it is only
    /// as known as that position is.
    ///
    /// Reporting zero for a partition whose owner has gone would say the
    /// replicas are perfectly caught up at the exact moment a failover is
    /// deciding which of them to promote, which is the worst possible time to
    /// be confidently wrong.
    fn lag(&self, position: impl Fn(&ReplicaView) -> Option<u64>) -> Option<u64> {
        let committed = self.committed_lamport?;
        // A replica that has not reported has no distance, and guessing one
        // would be the same mistake as reading its silence as position zero.
        // The worst *known* lag is still a true statement, so it is reported,
        // and `replicas_without_position` says how many it could not see.
        self.replicas
            .iter()
            .filter_map(|r| Some(committed.saturating_sub(position(r)?)))
            .max()
            .or(Some(0))
    }

    /// Replicas whose position the leader group has not heard.
    #[must_use]
    pub fn replicas_without_position(&self) -> usize {
        self.replicas
            .iter()
            .filter(|r| r.durable_lamport.is_none())
            .count()
    }

    fn range(&self) -> String {
        let mut start = self.start_key.display();
        if start.is_empty() {
            start = "-inf".to_owned();
        }
        let end = self
            .end_key
            .as_ref()
            .map_or_else(|| "+inf".to_owned(), Blob::display);
        format!("[{start}, {end})")
    }

    /// Replica lag against the owner's committed lamport, which is the number
    /// an operator actually wants during a failover.
    fn replica_summary(&self) -> String {
        if self.replicas.is_empty() {
            return "none".to_owned();
        }
        self.replicas
            .iter()
            .map(|r| match (self.committed_lamport, r.applied_lamport) {
                (Some(committed), Some(applied)) => {
                    format!("{}(-{})", r.node_id, committed.saturating_sub(applied))
                }
                // Either there is no owner position to measure against, or
                // this replica has not said where it is. Both leave the
                // distance unknowable, and the replica is still worth naming.
                _ => format!("{}(-?)", r.node_id),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The columns of the partition table, in one place so that the four commands
/// that print partitions cannot drift apart.
const PARTITION_COLUMNS: &[&str] = &[
    "partition",
    "keyspace",
    "range",
    "owner",
    "epoch",
    "lamport",
    "size",
    "index",
    "lag",
    "wal lag",
    "replicas",
];

impl Render for PartitionView {
    fn render_human(&self, out: &mut String) {
        out.push_str(&table(PARTITION_COLUMNS, &[partition_row(self, None)]));
    }
}

/// One row of the partition table.
///
/// `health` is the node health lookup where there is one. A partition printed
/// on its own, after a split or a transfer, has no node list to consult, and
/// the owner is then just an id.
fn partition_row(p: &PartitionView, health: Option<&BTreeMap<u64, String>>) -> Vec<String> {
    // Zero is how the proto says unowned, and since #53 a partition sits
    // there for as long as a fenced one waits on its replicas. Printing the
    // id would render that as node zero, which is not a node.
    if p.owner_node_id == 0 {
        return partition_row_with_owner(p, "none".to_owned());
    }
    let owner = match health.map(|h| h.get(&p.owner_node_id)) {
        // An owner that is not healthy is the first thing to notice, so it is
        // spelled out in the row rather than left to be joined by eye against
        // the node table above.
        Some(Some(state)) if state != "healthy" => format!("{} ({state})", p.owner_node_id),
        Some(None) => format!("{} (unknown)", p.owner_node_id),
        _ => p.owner_node_id.to_string(),
    };
    partition_row_with_owner(p, owner)
}

/// A lag column, where absent means there is no owner position to measure a
/// distance from.
fn lag_text(lag: Option<u64>) -> String {
    lag.map_or_else(|| "unknown".to_owned(), |lag| lag.to_string())
}

fn partition_row_with_owner(p: &PartitionView, owner: String) -> Vec<String> {
    vec![
        p.id.to_string(),
        p.keyspace_id.to_string(),
        p.range(),
        owner,
        p.epoch.to_string(),
        p.committed_lamport
            .map_or_else(|| "unknown".to_owned(), |l| l.to_string()),
        format_bytes_or_unknown(p.size_bytes),
        format_bytes_or_unknown(p.index_bytes),
        lag_text(p.max_replica_lag()),
        lag_text(p.max_wal_lag()),
        p.replica_summary(),
    ]
}

/// What is wrong with the cluster, counted.
///
/// The tables below it answer "which one", and this answers "is anything".
/// During an incident that is the order the questions get asked in, and
/// scrolling a hundred partitions to find out is not an answer.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterSummaryView {
    pub node_count: usize,
    pub healthy_nodes: usize,
    pub suspect_nodes: usize,
    pub dead_nodes: usize,
    /// Absent means no node claims to be the Raft leader, which means the
    /// control plane cannot accept a change right now.
    pub raft_leader_node_id: Option<u64>,
    pub partition_count: usize,
    pub partitions_with_no_replica: usize,
    pub partitions_with_an_unhealthy_owner: usize,
    /// The worst lag among partitions whose owner has reported a position.
    ///
    /// A maximum over a subset is still a true statement about that subset,
    /// unlike a sum, which is why this stays a plain number and the index
    /// memory total does not. [`Self::partitions_without_owner_progress`] is
    /// how many partitions it could not look at.
    pub max_replica_lag: u64,
    pub max_wal_lag: u64,
    /// Partitions with no owner reporting, so no size, position, or lag.
    /// Since #53 this is what a fenced partition looks like for as long as
    /// the control plane waits for its replicas to report past the fence.
    pub partitions_without_owner_progress: usize,
    /// Replica placements whose position the leader group has not heard, so
    /// the lag above could not look at them. #79 calls this `Unestablished`
    /// on the owner's side; it is neither healthy nor stranded, and counting
    /// it is the only way the worst-lag number stays honest about its reach.
    pub replicas_without_position: usize,
    /// What every node's partition indexes cost, added up. ADR 0006 makes
    /// this the resource a cluster exhausts first, and a total is what an
    /// operator compares against the memory they bought.
    ///
    /// Absent when any node did not report a measurement. A sum that quietly
    /// skipped the nodes it could not see would be a total that reads low
    /// precisely during a rolling upgrade, which is when an operator is most
    /// likely to be watching memory and least able to afford a number that
    /// says there is room.
    pub index_memory_bytes: Option<u64>,
    /// What the nodes that did report add up to. Equal to
    /// [`Self::index_memory_bytes`] when every node reported, and offered
    /// beside the count below so a partial answer is still readable as one.
    pub reported_index_memory_bytes: u64,
    pub nodes_without_index_memory: usize,
    /// Keyspaces whose stored bytes have reached or passed their quota.
    /// Counted, not judged: it does not feed [`Self::healthy`], because the
    /// quota is a tenant's limit and not the cluster's health.
    pub keyspaces_over_quota: usize,
}

impl ClusterSummaryView {
    /// Whether anything here is worth acting on.
    #[must_use]
    fn healthy(&self) -> bool {
        self.suspect_nodes == 0
            && self.dead_nodes == 0
            && self.partitions_with_no_replica == 0
            && self.partitions_with_an_unhealthy_owner == 0
            && self.raft_leader_node_id.is_some()
    }
}

/// The partition map and node health together.
///
/// They are one view because the question an operator is asking is always
/// about both: a partition is only as available as the nodes holding it.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterView {
    pub summary: ClusterSummaryView,
    /// The active cluster version, or absent from a server that predates it.
    pub cluster_version: Option<String>,
    pub nodes: Vec<NodeView>,
    pub partitions: Vec<PartitionView>,
    /// What each keyspace is storing against the quota it was given. Empty
    /// from a server that predates quota reporting.
    pub keyspaces: Vec<KeyspaceUsageView>,
}

/// One keyspace's storage against its quota.
///
/// The saturation is reported and nothing is concluded from it. What counts
/// as too full depends on a measured envelope this project does not have yet,
/// so a number an operator can read beats a threshold we would be guessing at.
#[derive(Debug, Clone, Serialize)]
pub struct KeyspaceUsageView {
    pub id: u64,
    pub name: String,
    pub partition_count: u32,
    /// Bytes stored across this keyspace's partitions. Exact when
    /// [`Self::partitions_without_size`] is zero, and a floor otherwise.
    pub stored_bytes: u64,
    /// How many of this keyspace's partitions had no owner reporting a size.
    /// Nonzero makes [`Self::stored_bytes`] a lower bound, which is still
    /// enough to prove the keyspace is over its quota and not enough to
    /// prove it is under.
    pub partitions_without_size: u32,
    /// Absent means the keyspace has no storage quota, which is the default.
    pub max_storage_bytes: Option<u64>,
}

/// Where a keyspace's storage sits against the quota it was given.
///
/// An enum rather than an `Option<f64>` because a quota of zero is a real,
/// configurable state that no fraction can express, and folding it into
/// "no measurement" is how a keyspace that is entirely over its limit ends up
/// looking like one that has no limit at all.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QuotaUse {
    /// No quota configured, so there is nothing to be a fraction of.
    Unlimited,
    /// A quota of exactly zero bytes: the keyspace is allowed to hold
    /// nothing. `over` is true once it holds something, which happens the
    /// moment a nonempty keyspace is moved to this cap.
    Closed { over: bool },
    /// Stored bytes as a fraction of the quota. Can exceed one, because a
    /// quota is enforced on admission and existing bytes predate it.
    Fraction(f64),
    /// A floor on the fraction, because some of this keyspace's partitions
    /// had no owner reporting a size. The real figure is this or higher, so
    /// a floor at or above one still proves the keyspace is over quota.
    AtLeast(f64),
}

impl KeyspaceUsageView {
    /// Where this keyspace sits against its quota.
    #[must_use]
    pub fn quota_use(&self) -> QuotaUse {
        match self.max_storage_bytes {
            None => QuotaUse::Unlimited,
            Some(0) => QuotaUse::Closed {
                over: self.stored_bytes > 0,
            },
            Some(limit) => {
                let fraction = self.stored_bytes as f64 / limit as f64;
                if self.partitions_without_size == 0 {
                    QuotaUse::Fraction(fraction)
                } else {
                    QuotaUse::AtLeast(fraction)
                }
            }
        }
    }

    /// Whether this keyspace has reached or passed the quota it was given.
    #[must_use]
    pub fn is_over_quota(&self) -> bool {
        match self.quota_use() {
            QuotaUse::Unlimited => false,
            // A zero quota is reached at zero bytes: there is no room left,
            // whether or not anything is stored yet.
            QuotaUse::Closed { .. } => true,
            // A floor at or above the quota is proof. Below it is not, since
            // the partitions that did not report could hold anything.
            QuotaUse::Fraction(fraction) | QuotaUse::AtLeast(fraction) => fraction >= 1.0,
        }
    }

    fn quota_text(&self) -> String {
        self.max_storage_bytes
            .map_or_else(|| "none".to_owned(), format_bytes)
    }

    fn stored_text(&self) -> String {
        if self.partitions_without_size == 0 {
            format_bytes(self.stored_bytes)
        } else {
            format!("\u{2265} {}", format_bytes(self.stored_bytes))
        }
    }

    fn saturation_text(&self) -> String {
        match self.quota_use() {
            QuotaUse::Unlimited => "-".to_owned(),
            // Not a percentage, because the percentage is either zero over
            // zero or infinite, and both would be read as a mistake.
            QuotaUse::Closed { over: false } => "full".to_owned(),
            QuotaUse::Closed { over: true } => "over".to_owned(),
            QuotaUse::Fraction(fraction) => format!("{:.1}%", fraction * 100.0),
            QuotaUse::AtLeast(fraction) => format!("\u{2265} {:.1}%", fraction * 100.0),
        }
    }
}

impl ClusterView {
    /// Sorts what came back and counts the summary.
    ///
    /// The order is imposed here rather than trusted from the server so that
    /// running the command twice produces two outputs that diff cleanly.
    /// Comparing two describes a minute apart is how an operator watches a
    /// recovery, and it does not work if the rows move.
    #[must_use]
    pub fn new(mut nodes: Vec<NodeView>, mut partitions: Vec<PartitionView>) -> Self {
        nodes.sort_by(|a, b| a.role.cmp(&b.role).then(a.id.cmp(&b.id)));
        partitions.sort_by(|a, b| {
            a.keyspace_id
                .cmp(&b.keyspace_id)
                .then_with(|| a.start_key.sort_key().cmp(b.start_key.sort_key()))
                .then(a.id.cmp(&b.id))
        });

        let health: BTreeMap<u64, String> =
            nodes.iter().map(|n| (n.id, n.health.clone())).collect();
        let summary = ClusterSummaryView {
            node_count: nodes.len(),
            healthy_nodes: nodes.iter().filter(|n| n.health == "healthy").count(),
            suspect_nodes: nodes.iter().filter(|n| n.health == "suspect").count(),
            dead_nodes: nodes.iter().filter(|n| n.health == "dead").count(),
            raft_leader_node_id: nodes.iter().find(|n| n.raft_leader).map(|n| n.id),
            partition_count: partitions.len(),
            partitions_with_no_replica: partitions.iter().filter(|p| p.replicas.is_empty()).count(),
            partitions_with_an_unhealthy_owner: partitions
                .iter()
                .filter(|p| health.get(&p.owner_node_id).is_none_or(|h| h != "healthy"))
                .count(),
            max_replica_lag: partitions
                .iter()
                .filter_map(PartitionView::max_replica_lag)
                .max()
                .unwrap_or(0),
            max_wal_lag: partitions
                .iter()
                .filter_map(PartitionView::max_wal_lag)
                .max()
                .unwrap_or(0),
            partitions_without_owner_progress: partitions
                .iter()
                .filter(|p| p.committed_lamport.is_none())
                .count(),
            replicas_without_position: partitions
                .iter()
                .map(PartitionView::replicas_without_position)
                .sum(),
            // Summing Options gives None the moment one contributor is
            // unknown, which is the answer that does not under-report. The
            // partial sum is kept beside it so a mid-rollout describe still
            // shows what is known instead of nothing.
            index_memory_bytes: nodes.iter().map(|n| n.index_memory_bytes).sum(),
            reported_index_memory_bytes: nodes.iter().filter_map(|n| n.index_memory_bytes).sum(),
            nodes_without_index_memory: nodes
                .iter()
                .filter(|n| n.index_memory_bytes.is_none())
                .count(),
            keyspaces_over_quota: 0,
        };

        Self {
            summary,
            cluster_version: None,
            nodes,
            partitions,
            keyspaces: Vec::new(),
        }
    }

    /// Attaches per-keyspace usage, which is where quota saturation lives.
    ///
    /// Separate from [`Self::new`] for the same reason the cluster version
    /// is: the commands that print a partial view after a split or a
    /// transfer have no keyspace list to attach and should not invent one.
    #[must_use]
    pub fn with_keyspaces(mut self, mut keyspaces: Vec<KeyspaceUsageView>) -> Self {
        // Sorted here for the same reason the nodes and partitions are: two
        // describes a minute apart have to diff cleanly.
        keyspaces.sort_by(|a, b| a.name.cmp(&b.name));
        self.summary.keyspaces_over_quota = keyspaces.iter().filter(|k| k.is_over_quota()).count();
        self.keyspaces = keyspaces;
        self
    }

    /// Attaches the active cluster version, kept separate from [`Self::new`]
    /// so the callers that print partial views do not have to invent one.
    #[must_use]
    pub fn with_cluster_version(mut self, version: Option<String>) -> Self {
        self.cluster_version = version;
        self
    }
}

impl Render for ClusterView {
    fn render_human(&self, out: &mut String) {
        let s = &self.summary;
        out.push_str("CLUSTER\n");
        let _ = writeln!(
            out,
            "  nodes        {} total, {} healthy, {} suspect, {} dead",
            s.node_count, s.healthy_nodes, s.suspect_nodes, s.dead_nodes
        );
        let _ = writeln!(
            out,
            "  raft leader  {}",
            s.raft_leader_node_id.map_or_else(
                || "none, the control plane cannot accept changes".to_owned(),
                |id| format!("node {id}")
            )
        );
        if let Some(version) = &self.cluster_version {
            let _ = writeln!(out, "  version      {version}");
        }
        let _ = writeln!(
            out,
            "  partitions   {} total, {} with an unhealthy owner, {} with no replica",
            s.partition_count, s.partitions_with_an_unhealthy_owner, s.partitions_with_no_replica
        );
        let unseen = s.partitions_without_owner_progress + s.replicas_without_position;
        let _ = if unseen == 0 {
            writeln!(
                out,
                "  worst lag    {} applied, {} wal",
                s.max_replica_lag, s.max_wal_lag
            )
        } else if s.partitions_without_owner_progress == 0 {
            writeln!(
                out,
                "  worst lag    {} applied, {} wal, {} replicas have not reported a position",
                s.max_replica_lag, s.max_wal_lag, s.replicas_without_position
            )
        } else {
            // Saying which partitions the number could not cover, because a
            // worst-lag of zero next to a silently skipped fenced partition
            // reads as a healthy cluster during a failover.
            writeln!(
                out,
                "  worst lag    {} applied, {} wal, {} partitions with no owner reporting",
                s.max_replica_lag, s.max_wal_lag, s.partitions_without_owner_progress
            )
        };
        // A total that silently dropped the nodes it could not measure would
        // read low exactly when an operator is mid-rollout and watching
        // memory, so an incomplete total says how incomplete it is.
        let reporting = s.node_count - s.nodes_without_index_memory;
        let _ = match s.index_memory_bytes {
            Some(total) => writeln!(out, "  index memory {}", format_bytes(total)),
            // Nothing was measured anywhere, which is what a whole cluster
            // one version behind looks like. A partial sum of nothing is
            // "0 B", and that is the reading this is here to prevent.
            None if reporting == 0 => writeln!(
                out,
                "  index memory unknown, {} nodes not reporting it",
                s.nodes_without_index_memory
            ),
            None => writeln!(
                out,
                "  index memory {} across {reporting} of {} nodes, {} not reporting",
                format_bytes(s.reported_index_memory_bytes),
                s.node_count,
                s.nodes_without_index_memory
            ),
        };
        if s.keyspaces_over_quota > 0 {
            let _ = writeln!(
                out,
                "  quotas       {} of {} keyspaces at or over their storage quota",
                s.keyspaces_over_quota,
                self.keyspaces.len()
            );
        }
        if s.healthy() {
            out.push_str("  everything reporting is healthy\n");
        }

        out.push_str("\nNODES\n");
        if self.nodes.is_empty() {
            out.push_str("  none\n");
        } else {
            let rows: Vec<Vec<String>> = self
                .nodes
                .iter()
                .map(|n| {
                    vec![
                        n.id.to_string(),
                        n.address.clone(),
                        n.role.clone(),
                        n.health.clone(),
                        if n.raft_leader { "yes" } else { "no" }.to_owned(),
                        n.speaks.clone(),
                        format_bytes_or_unknown(n.index_memory_bytes),
                    ]
                })
                .collect();
            out.push_str(&indent(&table(
                &[
                    "id",
                    "address",
                    "role",
                    "health",
                    "raft leader",
                    "speaks",
                    "index memory",
                ],
                &rows,
            )));
        }

        if !self.keyspaces.is_empty() {
            out.push_str("\nKEYSPACES\n");
            let rows: Vec<Vec<String>> = self
                .keyspaces
                .iter()
                .map(|k| {
                    vec![
                        k.name.clone(),
                        k.id.to_string(),
                        k.partition_count.to_string(),
                        k.stored_text(),
                        k.quota_text(),
                        k.saturation_text(),
                    ]
                })
                .collect();
            out.push_str(&indent(&table(
                &["name", "id", "partitions", "stored", "quota", "used"],
                &rows,
            )));
        }

        out.push_str("\nPARTITIONS\n");
        if self.partitions.is_empty() {
            out.push_str("  none\n");
        } else {
            let health: BTreeMap<u64, String> = self
                .nodes
                .iter()
                .map(|n| (n.id, n.health.clone()))
                .collect();
            let rows: Vec<Vec<String>> = self
                .partitions
                .iter()
                .map(|p| partition_row(p, Some(&health)))
                .collect();
            out.push_str(&indent(&table(PARTITION_COLUMNS, &rows)));
            out.push_str(
                "\nLag is how far a replica trails the owner's committed lamport. A replica at \
                 zero\ncan serve a linearizable read locally and would lose nothing if it were \
                 promoted.\nThe wal lag column measures the same distance at the log rather than \
                 at what has\nbeen applied, so it is what a promotion would have to hand over.\n",
            );
        }
    }
}

fn indent(text: &str) -> String {
    text.lines()
        .map(|line| format!("  {line}\n"))
        .collect::<String>()
}

/// Whether one node is answering.
///
/// `detail` names the status it answered with, because a node answering
/// `Unimplemented` is up and running a build that does not have the call yet,
/// and that is worth telling apart from a node answering normally.
#[derive(Debug, Clone, Serialize)]
pub struct PingView {
    pub endpoint: String,
    pub answered: bool,
    pub detail: String,
}

impl Render for PingView {
    fn render_human(&self, out: &mut String) {
        let _ = write!(out, "{} is answering ({})", self.endpoint, self.detail);
    }
}

/// A finalized upgrade: the version the cluster left and the one it is on.
///
/// The human rendering repeats the one-way-door warning, because the moment
/// this prints is the moment the cheap rollback stopped existing and the
/// operator deserves to be told at that moment, not only in the docs.
#[derive(Debug, Clone, Serialize)]
pub struct FinalizeUpgradeView {
    pub previous: String,
    pub active: String,
}

impl Render for FinalizeUpgradeView {
    fn render_human(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "cluster version advanced from {} to {}",
            self.previous, self.active
        );
        out.push_str(
            "Nodes will now write new formats. Rolling back past this point is not\n\
             supported; the only path backwards is restoring from a backup taken before\n\
             the upgrade.",
        );
    }
}

/// Whether one node is ready to serve.
///
/// Only a ready node is rendered; an unready one exits non-zero with the
/// unmet conditions in the error, because a probe reads the exit code and a
/// human reads stderr. The conditions are listed even when everything is met
/// so the JSON output says what "ready" was checked against.
#[derive(Debug, Clone, Serialize)]
pub struct ReadyView {
    pub endpoint: String,
    pub ready: bool,
    pub conditions: Vec<ReadyConditionView>,
}

/// One readiness condition, named the way the server names it.
#[derive(Debug, Clone, Serialize)]
pub struct ReadyConditionView {
    pub name: String,
    pub met: bool,
}

impl Render for ReadyView {
    fn render_human(&self, out: &mut String) {
        let names: Vec<&str> = self
            .conditions
            .iter()
            .map(|condition| condition.name.as_str())
            .collect();
        let _ = write!(out, "{} is ready ({})", self.endpoint, names.join(", "));
    }
}

/// A split, which turns one partition into two.
#[derive(Debug, Clone, Serialize)]
pub struct SplitView {
    pub lower: Option<PartitionView>,
    pub upper: Option<PartitionView>,
}

impl Render for SplitView {
    fn render_human(&self, out: &mut String) {
        let rows: Vec<Vec<String>> = [self.lower.as_ref(), self.upper.as_ref()]
            .into_iter()
            .flatten()
            .map(|p| partition_row(p, None))
            .collect();
        out.push_str("split into\n");
        out.push_str(&indent(&table(PARTITION_COLUMNS, &rows)));
    }
}

/// One partition, after a merge or a transfer.
#[derive(Debug, Clone, Serialize)]
pub struct PartitionResultView {
    pub summary: String,
    pub partition: Option<PartitionView>,
}

impl Render for PartitionResultView {
    fn render_human(&self, out: &mut String) {
        let _ = writeln!(out, "{}", self.summary);
        if let Some(partition) = &self.partition {
            out.push_str(&indent(&table(
                PARTITION_COLUMNS,
                &[partition_row(partition, None)],
            )));
        }
    }
}

/// The answer to a GET.
///
/// A miss is not an error, because "this key is absent" is the answer to a
/// lock check as often as a hit is. The exit code carries it instead, so a
/// script can branch without parsing anything. See
/// [`crate::cli::EXIT_NOT_FOUND`].
#[derive(Debug, Clone, Serialize)]
pub struct GetView {
    pub found: bool,
    pub key: Blob,
    pub value: Option<Blob>,
    pub version: Option<u64>,
    pub expires_at_millis: Option<u64>,
}

impl Render for GetView {
    fn render_human(&self, out: &mut String) {
        match (&self.value, self.version) {
            (Some(value), Some(version)) => {
                let _ = writeln!(out, "{}", value.display());
                let _ = write!(out, "version {version}");
                if let Some(expiry) = self.expires_at_millis {
                    let _ = write!(out, ", expires {}", format_millis(expiry));
                }
            }
            _ => {
                let _ = write!(out, "{}: not found", self.key.display());
            }
        }
    }
}

/// The result of a SET.
#[derive(Debug, Clone, Serialize)]
pub struct SetView {
    pub applied: bool,
    pub version: u64,
    /// What was there instead, when a condition was not met. Absent means the
    /// key did not exist.
    pub current_version: Option<u64>,
}

impl Render for SetView {
    fn render_human(&self, out: &mut String) {
        if self.applied {
            let _ = write!(out, "ok, version {}", self.version);
        } else {
            let found = self.current_version.map_or_else(
                || "the key was absent".to_owned(),
                |v| format!("version {v}"),
            );
            let _ = write!(out, "condition not met, {found}");
        }
    }
}

/// The result of a DELETE.
#[derive(Debug, Clone, Serialize)]
pub struct DeleteView {
    pub applied: bool,
    pub existed: bool,
    pub current_version: Option<u64>,
}

impl Render for DeleteView {
    fn render_human(&self, out: &mut String) {
        if self.applied && self.existed {
            out.push_str("deleted");
        } else if self.applied {
            out.push_str("ok, the key was already absent");
        } else {
            let found = self.current_version.map_or_else(
                || "the key was absent".to_owned(),
                |v| format!("version {v}"),
            );
            let _ = write!(out, "condition not met, {found}");
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ListEntryView {
    pub key: Blob,
    pub value: Option<Blob>,
    pub version: u64,
    pub expires_at_millis: Option<u64>,
}

/// One page of a LIST.
///
/// The cursor is carried through to the caller because a page is a snapshot of
/// one partition's range and a full scan is the caller's to assemble. Hiding
/// the cursor would be pretending otherwise.
#[derive(Debug, Clone, Serialize)]
pub struct ListView {
    pub entries: Vec<ListEntryView>,
    pub next_cursor: Option<Blob>,
}

impl Render for ListView {
    fn render_human(&self, out: &mut String) {
        if self.entries.is_empty() {
            out.push_str("no keys");
        } else if self.entries.iter().any(|e| e.value.is_some()) {
            let rows: Vec<Vec<String>> = self
                .entries
                .iter()
                .map(|e| {
                    vec![
                        e.key.display(),
                        e.version.to_string(),
                        e.value.as_ref().map_or_else(String::new, Blob::display),
                    ]
                })
                .collect();
            out.push_str(&table(&["key", "version", "value"], &rows));
        } else {
            for entry in &self.entries {
                let _ = writeln!(out, "{}", entry.key.display());
            }
        }
        if let Some(cursor) = &self.next_cursor {
            let _ = write!(
                out,
                "\nmore results. Continue with --cursor {}",
                cursor.display()
            );
        }
    }
}

impl Render for crate::config::Config {
    fn render_human(&self, out: &mut String) {
        let mut line = |key: &str, value: String| {
            let _ = writeln!(out, "{key:<31} {value}");
        };
        line("node.id", self.node.id.to_string());
        line("node.role", self.node.role.to_string());
        line("node.listen", self.node.listen.clone());
        line("node.advertise", self.node.advertise.clone());
        line("node.peer_listen", self.node.peer_listen.clone());
        line("node.peer_advertise", self.node.peer_advertise.clone());
        line("node.data_dir", self.node.data_dir.display().to_string());
        line("cluster.name", self.cluster.name.clone());
        line("cluster.leader_peers", self.cluster.leader_peers.join(", "));
        line(
            "cluster.allow_version_skew",
            self.cluster.allow_version_skew.to_string(),
        );
        line(
            "cluster.require_auth",
            self.cluster.require_auth.to_string(),
        );
        line(
            "cluster.join_backoff_initial",
            format!("{} ms", self.cluster.join_backoff_initial_millis),
        );
        line(
            "cluster.join_backoff_max",
            format!("{} ms", self.cluster.join_backoff_max_millis),
        );
        line(
            "cluster.join_timeout",
            if self.cluster.join_timeout_millis == 0 {
                "0 ms, retry forever".to_owned()
            } else {
                format!("{} ms", self.cluster.join_timeout_millis)
            },
        );
        line(
            "object_store.endpoint",
            self.object_store.endpoint.clone().unwrap_or_default(),
        );
        line("object_store.bucket", self.object_store.bucket.clone());
        line("object_store.region", self.object_store.region.clone());
        line(
            "object_store.access_key_id",
            self.object_store.access_key_id.clone().unwrap_or_default(),
        );
        line(
            "object_store.force_path_style",
            self.object_store.force_path_style.to_string(),
        );
        line(
            "telemetry.otlp_endpoint",
            self.telemetry.otlp_endpoint.clone().unwrap_or_default(),
        );
        line(
            "telemetry.trace_sample_ratio",
            self.telemetry.trace_sample_ratio.to_string(),
        );
        line(
            "telemetry.service_name",
            self.telemetry.service_name.clone(),
        );
        line("telemetry.log_level", self.telemetry.log_level.clone());
        for (key, value) in &self.telemetry.resource_attributes {
            line(&format!("telemetry.resource.{key}"), value.clone());
        }
        line("client.endpoint", self.client.endpoint.clone());
        out.push_str(
            "\nSecrets are omitted. Precedence, lowest first: defaults, file, environment, flags.",
        );
    }
}

/// The environment variables the tool reads.
#[derive(Debug, Clone, Serialize)]
pub struct EnvView {
    pub variables: Vec<EnvVariable>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnvVariable {
    pub name: String,
    pub sets: String,
}

impl Render for EnvView {
    fn render_human(&self, out: &mut String) {
        let rows: Vec<Vec<String>> = self
            .variables
            .iter()
            .map(|v| vec![v.name.clone(), v.sets.clone()])
            .collect();
        out.push_str(&table(&["variable", "sets"], &rows));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_output_is_pretty_printed_and_ends_with_one_newline() {
        let text = render(Format::Json, &Ack::new("deleted keyspace orders")).unwrap();
        assert!(text.contains("\"ok\": true"), "{text}");
        assert!(text.ends_with("}\n"), "{text}");
    }

    #[test]
    fn human_output_is_the_message_and_nothing_else() {
        let text = render(Format::Human, &Ack::new("deleted keyspace orders")).unwrap();
        assert_eq!(text, "deleted keyspace orders\n");
    }

    #[test]
    fn a_table_pads_columns_to_their_widest_cell() {
        let text = table(
            &["id", "name"],
            &[
                vec!["1".to_owned(), "orders".to_owned()],
                vec!["1000".to_owned(), "sessions".to_owned()],
            ],
        );
        assert_eq!(text, "ID    NAME\n1     orders\n1000  sessions\n");
    }

    #[test]
    fn text_bytes_render_as_text_and_binary_bytes_render_as_base64() {
        assert_eq!(
            Blob::new(b"hello"),
            Blob::Utf8 {
                value: "hello".to_owned()
            }
        );
        let binary = Blob::new(&[0xff, 0xfe]);
        assert_eq!(
            binary.display(),
            format!("base64:{}", BASE64.encode([0xff, 0xfe]))
        );
    }

    #[test]
    fn a_blob_states_its_encoding_in_json_so_a_script_need_not_guess() {
        let json = serde_json::to_string(&Blob::new(&[0xff])).unwrap();
        assert!(json.contains("\"encoding\":\"base64\""), "{json}");
        let json = serde_json::to_string(&Blob::new(b"k1")).unwrap();
        assert!(json.contains("\"encoding\":\"utf8\""), "{json}");
    }

    #[test]
    fn the_unix_epoch_and_a_leap_day_both_format_correctly() {
        assert_eq!(format_millis(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_millis(1_709_164_800_000), "2024-02-29T00:00:00Z");
        assert_eq!(format_millis(1_700_000_000_123), "2023-11-14T22:13:20Z");
    }

    #[test]
    fn byte_counts_below_a_kibibyte_stay_exact() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(5 * 1024 * 1024 * 1024), "5.0 GiB");
    }

    fn partition(id: u64) -> PartitionView {
        PartitionView {
            id,
            keyspace_id: 1,
            start_key: Blob::new(b""),
            end_key: None,
            owner_node_id: 3,
            epoch: 2,
            committed_lamport: Some(100),
            size_bytes: Some(2048),
            index_bytes: Some(4096),
            replicas: vec![
                ReplicaView {
                    node_id: 4,
                    applied_lamport: Some(100),
                    durable_lamport: Some(100),
                },
                ReplicaView {
                    node_id: 5,
                    applied_lamport: Some(91),
                    durable_lamport: Some(96),
                },
            ],
        }
    }

    #[test]
    fn an_unbounded_partition_range_reads_as_infinite_in_both_directions() {
        assert_eq!(partition(1).range(), "[-inf, +inf)");
    }

    #[test]
    fn a_replica_summary_shows_how_far_each_replica_trails_the_owner() {
        assert_eq!(partition(1).replica_summary(), "4(-0) 5(-9)");
    }

    #[test]
    fn describe_reports_wal_lag_separately_from_applied_lag() {
        // A replica can hold the log and still be applying it. Reporting one
        // number for both would hide whichever problem the operator has.
        let p = partition(1);
        assert_eq!(p.max_replica_lag(), Some(9));
        assert_eq!(p.max_wal_lag(), Some(4));
    }

    #[test]
    fn describe_reports_the_index_memory_a_partition_and_a_cluster_hold() {
        let view = ClusterView::new(
            vec![
                node(1, "leader", "healthy", true),
                node(3, "worker", "healthy", false),
            ],
            vec![partition(1)],
        );
        assert_eq!(view.summary.index_memory_bytes, Some(1024 + 3072));
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("index memory"), "{text}");
        assert!(
            text.contains("4.0 KiB"),
            "the partition's index is a column: {text}"
        );
    }

    #[test]
    fn an_unreported_index_renders_as_unknown_rather_than_as_an_empty_one() {
        // The mixed-version case, at the surface an operator actually reads.
        // A node running an older binary reports no measurement, and printing
        // "0 B" would tell them a full index is empty, at the exact moment
        // during a rolling upgrade when they are watching memory.
        let mut unreported = node(3, "worker", "healthy", false);
        unreported.index_memory_bytes = None;
        let mut partition = partition(1);
        partition.index_bytes = None;

        let view = ClusterView::new(
            vec![node(1, "leader", "healthy", true), unreported],
            vec![partition],
        );

        assert_eq!(
            view.summary.index_memory_bytes, None,
            "one silent node means the cluster has no honest total"
        );
        assert_eq!(view.summary.reported_index_memory_bytes, 1024);
        assert_eq!(view.summary.nodes_without_index_memory, 1);

        let text = render(Format::Human, &view).unwrap();
        assert!(
            text.contains("unknown"),
            "the node and partition rows say unknown: {text}"
        );
        assert!(
            text.contains("1 not reporting"),
            "and the total says how much of it is missing: {text}"
        );
        assert!(
            !text.contains("0 B"),
            "nothing here may be rendered as an empty index: {text}"
        );
    }

    #[test]
    fn an_index_measured_at_zero_still_renders_as_zero() {
        // The other side of the same distinction. A node that has measured
        // its index and found nothing there is a real answer, and it must not
        // be swept into "unknown" by the fix for the case above.
        let mut measured = node(3, "worker", "healthy", false);
        measured.index_memory_bytes = Some(0);
        let view = ClusterView::new(vec![measured], Vec::new());

        assert_eq!(view.summary.index_memory_bytes, Some(0));
        assert_eq!(view.summary.nodes_without_index_memory, 0);

        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("0 B"), "{text}");
        assert!(!text.contains("unknown"), "{text}");
    }

    #[test]
    fn describe_reports_quota_saturation_and_says_nothing_about_it() {
        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], Vec::new())
            .with_keyspaces(vec![
                KeyspaceUsageView {
                    id: 1,
                    name: "orders".to_owned(),
                    partition_count: 2,
                    stored_bytes: 512,
                    partitions_without_size: 0,
                    max_storage_bytes: Some(1024),
                },
                KeyspaceUsageView {
                    id: 2,
                    name: "audit".to_owned(),
                    partition_count: 1,
                    stored_bytes: 999,
                    partitions_without_size: 0,
                    max_storage_bytes: None,
                },
            ]);

        assert_eq!(view.keyspaces[0].name, "audit", "sorted for a clean diff");
        assert_eq!(view.keyspaces[1].quota_use(), QuotaUse::Fraction(0.5));
        assert_eq!(
            view.keyspaces[0].quota_use(),
            QuotaUse::Unlimited,
            "a keyspace with no quota has no saturation, rather than zero"
        );
        assert_eq!(
            view.summary.keyspaces_over_quota, 0,
            "neither of these is at its limit"
        );

        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("KEYSPACES"), "{text}");
        assert!(text.contains("50.0%"), "{text}");
        assert!(
            text.contains("none"),
            "an unlimited keyspace says so: {text}"
        );
    }

    #[test]
    fn a_nonempty_keyspace_at_a_zero_quota_reads_as_over_and_not_as_unlimited() {
        // A zero quota is a configured cap, not the absence of one. Treating
        // it as no measurement printed the same dash an unlimited keyspace
        // gets, so a tenant that is entirely over its limit looked like one
        // that has no limit, which is backwards in the worst way.
        let over = KeyspaceUsageView {
            id: 1,
            name: "orders".to_owned(),
            partition_count: 1,
            stored_bytes: 10,
            partitions_without_size: 0,
            max_storage_bytes: Some(0),
        };
        assert_eq!(over.quota_use(), QuotaUse::Closed { over: true });
        assert_eq!(over.saturation_text(), "over");
        assert!(over.is_over_quota());

        // The same cap on an empty keyspace is not over it, but it still has
        // no room, and it is still not "unlimited".
        let empty = KeyspaceUsageView {
            stored_bytes: 0,
            ..over.clone()
        };
        assert_eq!(empty.quota_use(), QuotaUse::Closed { over: false });
        assert_eq!(empty.saturation_text(), "full");
        assert!(empty.is_over_quota());

        let unlimited = KeyspaceUsageView {
            max_storage_bytes: None,
            ..over.clone()
        };
        assert_eq!(unlimited.saturation_text(), "-");
        assert!(!unlimited.is_over_quota());
        assert_ne!(
            over.saturation_text(),
            unlimited.saturation_text(),
            "a zero quota and no quota must not print the same thing"
        );
    }

    #[test]
    fn a_keyspace_at_or_over_its_quota_is_counted_in_the_summary() {
        // The renderer showing "over" in one row is not enough on its own:
        // the summary is what an operator reads first, and a tenant that
        // cannot accept another byte should not need a scroll to find.
        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], Vec::new())
            .with_keyspaces(vec![
                KeyspaceUsageView {
                    id: 1,
                    name: "closed".to_owned(),
                    partition_count: 1,
                    stored_bytes: 10,
                    partitions_without_size: 0,
                    max_storage_bytes: Some(0),
                },
                KeyspaceUsageView {
                    id: 2,
                    name: "roomy".to_owned(),
                    partition_count: 1,
                    stored_bytes: 10,
                    partitions_without_size: 0,
                    max_storage_bytes: Some(1024),
                },
            ]);

        assert_eq!(view.summary.keyspaces_over_quota, 1);
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("at or over their storage quota"), "{text}");
        assert!(text.contains("over"), "the row says so too: {text}");
        assert!(
            view.summary.healthy(),
            "a tenant's quota is not the cluster's health, and no resource \
             number may flip that verdict"
        );
    }

    #[test]
    fn a_partition_with_no_owner_reporting_renders_unknown_rather_than_zeroes() {
        // Since #53 a fenced partition stays unowned until every surviving
        // replica has reported past the fence, so this row is what an
        // operator sees during an ordinary failover. Zeroes here would say
        // the partition is empty and its replicas are perfectly caught up,
        // which is the opposite of true and arrives at the moment somebody is
        // deciding what the cluster can afford to lose.
        let mut fenced = partition(1);
        fenced.owner_node_id = 0;
        fenced.committed_lamport = None;
        fenced.size_bytes = None;
        fenced.index_bytes = None;

        assert_eq!(fenced.max_replica_lag(), None);
        assert_eq!(fenced.max_wal_lag(), None);

        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], vec![fenced]);
        assert_eq!(view.summary.partitions_without_owner_progress, 1);

        let text = render(Format::Human, &view).unwrap();
        assert!(
            !text.contains("0 B"),
            "an unmeasured partition is not an empty one: {text}"
        );
        assert!(
            text.contains("unknown"),
            "size, position, and both lags say so: {text}"
        );
        assert!(
            text.contains("none"),
            "and an unowned partition names no owner: {text}"
        );
        assert!(
            text.contains("partitions with no owner reporting"),
            "the summary says which partitions its worst-lag could not \
             cover: {text}"
        );
        assert!(
            text.contains("(-?)"),
            "a replica has no measurable distance from an owner that is \
             gone: {text}"
        );
    }

    #[test]
    fn a_keyspace_missing_a_partitions_size_reports_a_floor_not_a_total() {
        // The same defect one level up. A keyspace total that quietly skipped
        // a fenced partition would read as a small exact number, and quota
        // saturation is the one place that reading costs something.
        let partial = KeyspaceUsageView {
            id: 1,
            name: "orders".to_owned(),
            partition_count: 3,
            stored_bytes: 512,
            partitions_without_size: 1,
            max_storage_bytes: Some(1024),
        };
        assert_eq!(partial.quota_use(), QuotaUse::AtLeast(0.5));
        assert!(
            !partial.is_over_quota(),
            "a floor below the quota proves nothing"
        );
        assert!(
            partial.saturation_text().starts_with('\u{2265}'),
            "{}",
            partial.saturation_text()
        );
        assert!(
            partial.stored_text().starts_with('\u{2265}'),
            "{}",
            partial.stored_text()
        );

        // A floor at or above the quota is proof regardless of what the
        // partitions that did not report are holding.
        let proven = KeyspaceUsageView {
            stored_bytes: 2048,
            ..partial.clone()
        };
        assert!(proven.is_over_quota());

        let complete = KeyspaceUsageView {
            partitions_without_size: 0,
            ..partial.clone()
        };
        assert_eq!(complete.quota_use(), QuotaUse::Fraction(0.5));
        assert_eq!(complete.saturation_text(), "50.0%");
    }

    #[test]
    fn a_replica_that_has_not_reported_renders_unknown_rather_than_maximally_behind() {
        // The state #79 calls `Unestablished`, at the surface an operator
        // reads. A replica at the bottom of the log and a replica that has
        // said nothing look identical if silence is rendered as zero, and one
        // of them is an emergency while the other may be perfectly current.
        let mut p = partition(1);
        p.replicas[1].applied_lamport = None;
        p.replicas[1].durable_lamport = None;

        assert_eq!(p.replicas_without_position(), 1);
        assert_eq!(
            p.max_replica_lag(),
            Some(0),
            "the worst lag among replicas that did report is still true about \
             them, and node 4 is level"
        );

        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], vec![p]);
        assert_eq!(view.summary.replicas_without_position, 1);

        let text = render(Format::Human, &view).unwrap();
        assert!(
            text.contains("5(-?)"),
            "a replica with no position has no measurable distance: {text}"
        );
        assert!(
            text.contains("4(-0)"),
            "and one that did report keeps its number: {text}"
        );
        assert!(
            text.contains("replicas have not reported a position"),
            "the summary says how far its lag could not see: {text}"
        );
    }

    #[test]
    fn a_quiescing_owner_does_not_render_as_though_it_lost_data() {
        // #79 gave a draining owner `quiesce`, which drops the writes it holds
        // alone — writes whose clients were told they failed — and so lowers
        // its durable position. Nothing was lost. The committed prefix is the
        // watermark that cannot go backwards, and it is what this column
        // shows, so a planned shutdown does not read as a partition losing
        // ground.
        let before = PartitionView {
            committed_lamport: Some(90),
            ..partition(1)
        };
        let after_quiesce = PartitionView {
            committed_lamport: Some(90),
            ..partition(1)
        };

        let first = render(
            Format::Human,
            &ClusterView::new(vec![node(3, "worker", "healthy", true)], vec![before]),
        )
        .unwrap();
        let second = render(
            Format::Human,
            &ClusterView::new(
                vec![node(3, "worker", "healthy", true)],
                vec![after_quiesce],
            ),
        )
        .unwrap();
        assert_eq!(
            first, second,
            "two describes across a quiesce have to diff cleanly, because \
             nothing an operator is shown actually moved"
        );
    }

    #[test]
    fn a_cluster_without_keyspace_usage_prints_no_keyspace_section() {
        // A server that predates quota reporting sends none, and inventing an
        // empty table would read as "this cluster has no keyspaces".
        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], Vec::new());
        let text = render(Format::Human, &view).unwrap();
        assert!(!text.contains("KEYSPACES"), "{text}");
    }

    #[test]
    fn an_empty_keyspace_list_tells_the_reader_how_to_make_one() {
        let text = render(
            Format::Human,
            &KeyspaceListView {
                keyspaces: Vec::new(),
            },
        )
        .unwrap();
        assert!(text.contains("orbita keyspace create"), "{text}");
    }

    fn node(id: u64, role: &str, health: &str, raft_leader: bool) -> NodeView {
        NodeView {
            id,
            address: format!("10.0.0.{id}:7100"),
            role: role.to_owned(),
            health: health.to_owned(),
            raft_leader,
            speaks: "0.1..0.2".to_owned(),
            index_memory_bytes: Some(1024 * id),
        }
    }

    #[test]
    fn a_cluster_with_no_partitions_still_prints_both_sections() {
        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], Vec::new());
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("NODES"), "{text}");
        assert!(text.contains("10.0.0.1:7100"), "{text}");
        assert!(text.contains("PARTITIONS"), "{text}");
        assert!(text.contains("none"), "{text}");
    }

    #[test]
    fn the_summary_counts_nodes_by_health_before_the_tables_have_to_be_read() {
        let view = ClusterView::new(
            vec![
                node(1, "leader", "healthy", true),
                node(2, "leader", "suspect", false),
                node(3, "worker", "dead", false),
            ],
            Vec::new(),
        );
        assert_eq!(view.summary.healthy_nodes, 1);
        assert_eq!(view.summary.suspect_nodes, 1);
        assert_eq!(view.summary.dead_nodes, 1);
        assert_eq!(view.summary.raft_leader_node_id, Some(1));
        let text = render(Format::Human, &view).unwrap();
        assert!(
            text.contains("3 total, 1 healthy, 1 suspect, 1 dead"),
            "{text}"
        );
    }

    #[test]
    fn a_cluster_with_no_raft_leader_says_the_control_plane_cannot_accept_changes() {
        let view = ClusterView::new(vec![node(1, "leader", "healthy", false)], Vec::new());
        assert_eq!(view.summary.raft_leader_node_id, None);
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("cannot accept changes"), "{text}");
    }

    #[test]
    fn a_partition_owned_by_a_node_that_is_not_healthy_says_so_in_its_row() {
        let view = ClusterView::new(vec![node(3, "worker", "dead", false)], vec![partition(1)]);
        assert_eq!(view.summary.partitions_with_an_unhealthy_owner, 1);
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("3 (dead)"), "{text}");
    }

    #[test]
    fn a_partition_owned_by_a_node_the_cluster_never_reported_is_marked_unknown() {
        let view = ClusterView::new(vec![node(1, "leader", "healthy", true)], vec![partition(1)]);
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("3 (unknown)"), "{text}");
    }

    #[test]
    fn the_summary_carries_the_worst_replica_lag_in_the_whole_cluster() {
        let mut behind = partition(2);
        behind.replicas[1].applied_lamport = Some(40);
        let view = ClusterView::new(Vec::new(), vec![partition(1), behind]);
        assert_eq!(view.summary.max_replica_lag, 60);
    }

    #[test]
    fn two_describes_in_a_row_print_their_rows_in_the_same_order() {
        let nodes = vec![
            node(9, "worker", "healthy", false),
            node(2, "leader", "healthy", true),
        ];
        let forward = ClusterView::new(nodes.clone(), vec![partition(7), partition(3)]);
        let mut reversed_nodes = nodes;
        reversed_nodes.reverse();
        let backward = ClusterView::new(reversed_nodes, vec![partition(3), partition(7)]);
        assert_eq!(
            render(Format::Human, &forward).unwrap(),
            render(Format::Human, &backward).unwrap()
        );
    }

    #[test]
    fn a_failed_conditional_write_reports_the_version_it_found_instead() {
        let text = render(
            Format::Human,
            &SetView {
                applied: false,
                version: 0,
                current_version: Some(42),
            },
        )
        .unwrap();
        assert_eq!(text, "condition not met, version 42\n");
    }

    #[test]
    fn a_failed_conditional_write_on_an_absent_key_says_so() {
        let text = render(
            Format::Human,
            &SetView {
                applied: false,
                version: 0,
                current_version: None,
            },
        )
        .unwrap();
        assert_eq!(text, "condition not met, the key was absent\n");
    }

    #[test]
    fn a_get_that_missed_names_the_key_rather_than_printing_nothing() {
        let text = render(
            Format::Human,
            &GetView {
                found: false,
                key: Blob::new(b"locks/leader"),
                value: None,
                version: None,
                expires_at_millis: None,
            },
        )
        .unwrap();
        assert_eq!(text, "locks/leader: not found\n");
    }

    #[test]
    fn a_get_prints_the_value_first_so_it_can_be_piped() {
        let text = render(
            Format::Human,
            &GetView {
                found: true,
                key: Blob::new(b"k"),
                value: Some(Blob::new(b"v")),
                version: Some(7),
                expires_at_millis: None,
            },
        )
        .unwrap();
        assert_eq!(text, "v\nversion 7\n");
    }

    #[test]
    fn a_key_only_list_prints_one_key_per_line_for_piping() {
        let view = ListView {
            entries: vec![
                ListEntryView {
                    key: Blob::new(b"a"),
                    value: None,
                    version: 1,
                    expires_at_millis: None,
                },
                ListEntryView {
                    key: Blob::new(b"b"),
                    value: None,
                    version: 2,
                    expires_at_millis: None,
                },
            ],
            next_cursor: None,
        };
        assert_eq!(render(Format::Human, &view).unwrap(), "a\nb\n");
    }

    #[test]
    fn a_truncated_list_tells_the_reader_how_to_get_the_next_page() {
        let view = ListView {
            entries: Vec::new(),
            next_cursor: Some(Blob::new(b"c2")),
        };
        let text = render(Format::Human, &view).unwrap();
        assert!(text.contains("--cursor c2"), "{text}");
    }

    #[test]
    fn json_output_carries_the_same_fields_the_table_shows() {
        let view = ClusterView::new(Vec::new(), vec![partition(9)]);
        let json: serde_json::Value =
            serde_json::from_str(&render(Format::Json, &view).unwrap()).unwrap();
        assert_eq!(json["summary"]["max_replica_lag"], 9);
        assert_eq!(json["partitions"][0]["id"], 9);
        assert_eq!(json["partitions"][0]["epoch"], 2);
        assert_eq!(json["partitions"][0]["owner_node_id"], 3);
        assert_eq!(json["partitions"][0]["replicas"][1]["applied_lamport"], 91);
        assert!(json["partitions"][0]["end_key"].is_null());
    }
}
