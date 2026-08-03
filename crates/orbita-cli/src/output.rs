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
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaView {
    pub node_id: u64,
    pub applied_lamport: u64,
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
    pub committed_lamport: u64,
    pub size_bytes: u64,
    pub replicas: Vec<ReplicaView>,
}

impl PartitionView {
    /// The furthest any replica trails the owner.
    ///
    /// This is the number that decides how much a failover would lose, so it
    /// gets a column of its own rather than being buried in the replica list.
    #[must_use]
    pub fn max_replica_lag(&self) -> u64 {
        self.replicas
            .iter()
            .map(|r| self.committed_lamport.saturating_sub(r.applied_lamport))
            .max()
            .unwrap_or(0)
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
            .map(|r| {
                let lag = self.committed_lamport.saturating_sub(r.applied_lamport);
                format!("{}(-{lag})", r.node_id)
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
    "lag",
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
    let owner = match health.map(|h| h.get(&p.owner_node_id)) {
        // An owner that is not healthy is the first thing to notice, so it is
        // spelled out in the row rather than left to be joined by eye against
        // the node table above.
        Some(Some(state)) if state != "healthy" => format!("{} ({state})", p.owner_node_id),
        Some(None) => format!("{} (unknown)", p.owner_node_id),
        _ => p.owner_node_id.to_string(),
    };
    vec![
        p.id.to_string(),
        p.keyspace_id.to_string(),
        p.range(),
        owner,
        p.epoch.to_string(),
        p.committed_lamport.to_string(),
        format_bytes(p.size_bytes),
        p.max_replica_lag().to_string(),
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
    pub max_replica_lag: u64,
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
    pub nodes: Vec<NodeView>,
    pub partitions: Vec<PartitionView>,
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
                .map(PartitionView::max_replica_lag)
                .max()
                .unwrap_or(0),
        };

        Self {
            summary,
            nodes,
            partitions,
        }
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
        let _ = writeln!(
            out,
            "  partitions   {} total, {} with an unhealthy owner, {} with no replica",
            s.partition_count, s.partitions_with_an_unhealthy_owner, s.partitions_with_no_replica
        );
        let _ = writeln!(out, "  worst lag    {}", s.max_replica_lag);
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
                    ]
                })
                .collect();
            out.push_str(&indent(&table(
                &["id", "address", "role", "health", "raft leader"],
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
                 promoted.\n",
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
            committed_lamport: 100,
            size_bytes: 2048,
            replicas: vec![
                ReplicaView {
                    node_id: 4,
                    applied_lamport: 100,
                },
                ReplicaView {
                    node_id: 5,
                    applied_lamport: 91,
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
        behind.replicas[1].applied_lamport = 40;
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
