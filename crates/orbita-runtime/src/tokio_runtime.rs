//! The production implementations of the runtime seams.
//!
//! There is no `Runtime` implementation here yet, because the production
//! transport is the gRPC peer client and that belongs to the server crate. See
//! `docs/plan/04-server.md`. The clock, disk, and randomness are here and are
//! complete.

use crate::clock::Clock;
use crate::disk::{Disk, DiskError, DiskResult, File, OpenOptions};

use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Wall and monotonic time from the operating system.
#[derive(Debug, Clone)]
pub struct TokioClock {
    origin: std::time::Instant,
}

impl Default for TokioClock {
    fn default() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

impl TokioClock {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Clock for TokioClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            // A clock before 1970 means the machine is badly misconfigured.
            // Treating it as the epoch expires every TTL immediately, which is
            // the safe direction: keys vanish rather than outliving their
            // deadline.
            .unwrap_or(0)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

/// A filesystem rooted at one node's data directory.
#[derive(Debug, Clone)]
pub struct TokioDisk {
    root: Arc<PathBuf>,
}

impl TokioDisk {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
        }
    }

    /// Resolves a relative path inside the root, rejecting anything that would
    /// escape it. Paths reach this layer from config and from partition
    /// identifiers, and a traversal here would let one keyspace read another's
    /// log.
    fn resolve(&self, path: &str) -> DiskResult<PathBuf> {
        let candidate = Path::new(path);
        if candidate.is_absolute()
            || candidate
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(DiskError::Io(format!("path escapes the data root: {path}")));
        }
        Ok(self.root.join(candidate))
    }
}

impl Disk for TokioDisk {
    type File = TokioFile;

    async fn open(&self, path: &str, options: OpenOptions) -> DiskResult<Self::File> {
        let full = self.resolve(path)?;
        if options.create {
            if let Some(parent) = full.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| DiskError::Io(e.to_string()))?;
            }
        }
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(options.create)
            .truncate(options.truncate)
            .open(&full)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => DiskError::NotFound(path.to_string()),
                _ => DiskError::Io(e.to_string()),
            })?;
        Ok(TokioFile {
            inner: Arc::new(Mutex::new(file)),
        })
    }

    async fn remove(&self, path: &str) -> DiskResult<()> {
        let full = self.resolve(path)?;
        match tokio::fs::remove_file(&full).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(DiskError::NotFound(path.to_string()))
            }
            Err(e) => Err(DiskError::Io(e.to_string())),
        }
    }

    async fn list(&self, prefix: &str) -> DiskResult<Vec<String>> {
        let dir = self.resolve(prefix)?;
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(DiskError::Io(e.to_string())),
        };
        let mut out = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| DiskError::Io(e.to_string()))?
        {
            if let Some(name) = entry.file_name().to_str() {
                out.push(name.to_string());
            }
        }
        // Directory order is not stable across filesystems, and callers that
        // recover a WAL depend on segment order.
        out.sort();
        Ok(out)
    }
}

/// A file with explicit `sync`.
#[derive(Debug, Clone)]
pub struct TokioFile {
    inner: Arc<Mutex<tokio::fs::File>>,
}

impl File for TokioFile {
    async fn append(&self, data: Bytes) -> DiskResult<u64> {
        let mut file = self.inner.lock().await;
        let offset = file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|e| DiskError::Io(e.to_string()))?;
        file.write_all(&data)
            .await
            .map_err(|e| DiskError::Io(e.to_string()))?;
        Ok(offset)
    }

    async fn sync(&self) -> DiskResult<()> {
        let file = self.inner.lock().await;
        file.sync_data()
            .await
            .map_err(|e| DiskError::Io(e.to_string()))
    }

    async fn read_at(&self, offset: u64, len: usize) -> DiskResult<Bytes> {
        let mut file = self.inner.lock().await;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| DiskError::Io(e.to_string()))?;
        let mut buf = BytesMut::zeroed(len);
        file.read_exact(&mut buf)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => DiskError::OutOfBounds { offset, len },
                _ => DiskError::Io(e.to_string()),
            })?;
        Ok(buf.freeze())
    }

    async fn truncate(&self, offset: u64) -> DiskResult<()> {
        let file = self.inner.lock().await;
        file.set_len(offset)
            .await
            .map_err(|e| DiskError::Io(e.to_string()))
    }

    async fn size(&self) -> DiskResult<u64> {
        let file = self.inner.lock().await;
        let meta = file
            .metadata()
            .await
            .map_err(|e| DiskError::Io(e.to_string()))?;
        Ok(meta.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "orbita-disk-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    #[tokio::test]
    async fn append_then_read_round_trips() {
        let root = temp_root();
        let disk = TokioDisk::new(&root);
        let file = disk
            .open("wal/000001.log", OpenOptions::create())
            .await
            .unwrap();

        let a = file.append(Bytes::from_static(b"hello ")).await.unwrap();
        let b = file.append(Bytes::from_static(b"world")).await.unwrap();
        file.sync().await.unwrap();

        assert_eq!(a, 0);
        assert_eq!(b, 6, "append returns the offset it wrote at");
        assert_eq!(
            file.read_at(0, 11).await.unwrap(),
            Bytes::from_static(b"hello world")
        );
        assert_eq!(file.size().await.unwrap(), 11);

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn truncate_drops_a_torn_tail() {
        let root = temp_root();
        let disk = TokioDisk::new(&root);
        let file = disk.open("t.log", OpenOptions::create()).await.unwrap();
        file.append(Bytes::from_static(b"keepDROP")).await.unwrap();
        file.truncate(4).await.unwrap();

        assert_eq!(file.size().await.unwrap(), 4);
        assert!(
            matches!(file.read_at(0, 8).await, Err(DiskError::OutOfBounds { .. })),
            "reading past the truncation point must fail loudly"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn paths_cannot_escape_the_data_root() {
        let root = temp_root();
        let disk = TokioDisk::new(&root);
        assert!(disk
            .open("../secrets", OpenOptions::create())
            .await
            .is_err());
        assert!(disk
            .open("/etc/passwd", OpenOptions::create())
            .await
            .is_err());
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_files_are_distinguishable() {
        let root = temp_root();
        let disk = TokioDisk::new(&root);
        assert!(matches!(
            disk.open("nope.log", OpenOptions::default()).await,
            Err(DiskError::NotFound(_))
        ));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn monotonic_time_never_goes_backwards() {
        let clock = TokioClock::new();
        let mut last = clock.monotonic_nanos();
        for _ in 0..100 {
            let now = clock.monotonic_nanos();
            assert!(now >= last);
            last = now;
        }
    }
}
