//! A disk that lies the way real disks lie.
//!
//! A file has two images: the bytes a reader sees now, and the bytes that
//! would survive a power cut. `sync` is what moves the first onto the second,
//! and the interesting fault is a `sync` that returns success without moving
//! anything. Real block devices with a volatile write cache do exactly that,
//! and a system that trusts `fsync` blindly loses acknowledged writes on a
//! power cut without ever seeing an error.
//!
//! The other faults model the same idea at a smaller scale: a write that fails
//! having written some of its bytes, and a crash that leaves the last record
//! half written. Recovery has to treat all three as normal.

use crate::world::{FileState, SimCore, SimSleep};

use orbita_core::NodeId;
use orbita_runtime::{Disk, DiskError, File, OpenOptions};

/// `orbita-runtime` defines this alias but does not re-export it from its
/// root, so it is spelled out again here rather than reaching into the
/// contract crate to change it.
type DiskResult<T> = Result<T, DiskError>;

use bytes::Bytes;
use std::sync::Arc;

/// One node's view of local storage.
#[derive(Clone)]
pub struct SimDisk {
    core: Arc<SimCore>,
    node: NodeId,
}

impl SimDisk {
    pub(crate) fn new(core: Arc<SimCore>, node: NodeId) -> Self {
        Self { core, node }
    }

    /// Charges an operation its latency. Every disk call pays this, which also
    /// means every disk call is a scheduling point, so a crash can land in the
    /// middle of a sequence of writes.
    async fn spin(&self) {
        let nanos = self.core.latency(
            self.core.config.disk.min_latency,
            self.core.config.disk.max_latency,
        );
        SimSleep::new(self.core.clone(), nanos).await;
    }
}

impl Disk for SimDisk {
    type File = SimFile;

    async fn open(&self, path: &str, options: OpenOptions) -> DiskResult<Self::File> {
        self.spin().await;
        let mut state = self.core.state();
        let node = state
            .nodes
            .get_mut(&self.node)
            .ok_or_else(|| DiskError::Io(format!("node {} does not exist", self.node)))?;
        let generation = node.disk_generation;
        let exists = node.files.contains_key(path);
        if !exists && !options.create {
            return Err(DiskError::NotFound(path.to_string()));
        }
        if options.truncate || !exists {
            node.files.insert(path.to_string(), FileState::default());
        }
        state.record(format!("disk open node={} path={path}", self.node));
        Ok(SimFile {
            core: self.core.clone(),
            node: self.node,
            path: path.to_string(),
            generation,
        })
    }

    async fn remove(&self, path: &str) -> DiskResult<()> {
        self.spin().await;
        let mut state = self.core.state();
        let node = state
            .nodes
            .get_mut(&self.node)
            .ok_or_else(|| DiskError::Io(format!("node {} does not exist", self.node)))?;
        if node.files.remove(path).is_none() {
            return Err(DiskError::NotFound(path.to_string()));
        }
        state.record(format!("disk remove node={} path={path}", self.node));
        Ok(())
    }

    async fn list(&self, dir: &str) -> DiskResult<Vec<String>> {
        self.spin().await;
        let state = self.core.state();
        let node = state
            .nodes
            .get(&self.node)
            .ok_or_else(|| DiskError::Io(format!("node {} does not exist", self.node)))?;

        // Files here are stored under their full path, so a directory listing
        // is the entries whose parent is this directory, reported as bare
        // names. Matching a prefix instead would report a nested file as
        // though it sat here, which is what this implementation used to do and
        // why the trait now says so at length.
        let prefix = if dir.is_empty() || dir.ends_with('/') {
            dir.to_string()
        } else {
            format!("{dir}/")
        };

        let mut names: Vec<String> = node
            .files
            .keys()
            .filter_map(|path| {
                let rest = path.strip_prefix(&prefix)?;
                // Anything with a separator left is in a subdirectory, and a
                // listing does not recurse.
                if rest.is_empty() || rest.contains('/') {
                    None
                } else {
                    Some(rest.to_string())
                }
            })
            .collect();

        // A real directory listing has no order, and recovery depends on
        // segment order, so sorting here means no caller has to remember to.
        names.sort();
        Ok(names)
    }
}

/// A handle to one simulated file.
///
/// The handle remembers which incarnation of the disk it came from, so a
/// handle held across a restart with a fresh disk fails loudly rather than
/// addressing whatever now lives at that path.
pub struct SimFile {
    core: Arc<SimCore>,
    node: NodeId,
    path: String,
    generation: u64,
}

impl SimFile {
    async fn spin(&self) {
        let nanos = self.core.latency(
            self.core.config.disk.min_latency,
            self.core.config.disk.max_latency,
        );
        SimSleep::new(self.core.clone(), nanos).await;
    }

    /// Runs `f` against this file's state, refusing if the node is gone, its
    /// disk has been replaced, or the file has been removed.
    fn with_file<T>(&self, f: impl FnOnce(&mut FileState) -> DiskResult<T>) -> DiskResult<T> {
        let mut state = self.core.state();
        let node = state
            .nodes
            .get_mut(&self.node)
            .ok_or_else(|| DiskError::Io(format!("node {} does not exist", self.node)))?;
        if node.disk_generation != self.generation {
            return Err(DiskError::Io(format!(
                "stale handle for {}: the disk was replaced",
                self.path
            )));
        }
        let file = node
            .files
            .get_mut(&self.path)
            .ok_or_else(|| DiskError::NotFound(self.path.clone()))?;
        f(file)
    }
}

impl File for SimFile {
    async fn append(&self, data: Bytes) -> DiskResult<u64> {
        self.spin().await;

        let fault = {
            let mut state = self.core.state();
            let faults = self.core.config.disk.clone();
            if self
                .core
                .roll_fault(&mut state, faults.write_failure_permille)
            {
                state.record(format!(
                    "disk fault node={} path={} kind=write-failure",
                    self.node, self.path
                ));
                Some(0usize)
            } else if self
                .core
                .roll_fault(&mut state, faults.partial_write_permille)
            {
                // At least one byte and never the whole record, since a write
                // that happened to complete is not a partial write.
                let kept = if data.len() <= 1 {
                    0
                } else {
                    1 + self.core.fault_below(data.len() as u64 - 1) as usize
                };
                state.record(format!(
                    "disk fault node={} path={} kind=partial-write kept={kept}",
                    self.node, self.path
                ));
                Some(kept)
            } else {
                None
            }
        };

        match fault {
            Some(kept) => self.with_file(|file| {
                file.visible.extend_from_slice(&data[..kept]);
                Err(DiskError::Io(format!(
                    "append of {} bytes failed after {kept}",
                    data.len()
                )))
            }),
            None => self.with_file(|file| {
                let offset = file.visible.len() as u64;
                file.visible.extend_from_slice(&data);
                Ok(offset)
            }),
        }
    }

    async fn sync(&self) -> DiskResult<()> {
        self.spin().await;
        let lying = {
            let mut state = self.core.state();
            let permille = self.core.config.disk.lying_fsync_permille;
            if self.core.roll_fault(&mut state, permille) {
                state.record(format!(
                    "disk fault node={} path={} kind=lying-fsync",
                    self.node, self.path
                ));
                true
            } else {
                false
            }
        };
        self.with_file(|file| {
            if !lying {
                file.durable.clone_from(&file.visible);
            }
            Ok(())
        })
    }

    async fn read_at(&self, offset: u64, len: usize) -> DiskResult<Bytes> {
        self.spin().await;
        let corrupt = {
            let mut state = self.core.state();
            let permille = self.core.config.disk.read_corruption_permille;
            if self.core.roll_fault(&mut state, permille) {
                state.record(format!(
                    "disk fault node={} path={} kind=read-corruption offset={offset}",
                    self.node, self.path
                ));
                true
            } else {
                false
            }
        };
        self.with_file(|file| {
            let start = offset as usize;
            let end = start
                .checked_add(len)
                .ok_or(DiskError::OutOfBounds { offset, len })?;
            if end > file.visible.len() {
                return Err(DiskError::OutOfBounds { offset, len });
            }
            if corrupt {
                // Reported rather than silently altered, because the contract
                // says a caller detects this by checksum and treats it as
                // truncation at that point.
                return Err(DiskError::Corrupt { offset });
            }
            Ok(Bytes::copy_from_slice(&file.visible[start..end]))
        })
    }

    async fn truncate(&self, offset: u64) -> DiskResult<()> {
        self.spin().await;
        self.with_file(|file| {
            let at = offset as usize;
            if at < file.visible.len() {
                file.visible.truncate(at);
            }
            if at < file.durable.len() {
                file.durable.truncate(at);
            }
            Ok(())
        })
    }

    async fn size(&self) -> DiskResult<u64> {
        self.spin().await;
        self.with_file(|file| Ok(file.visible.len() as u64))
    }
}
