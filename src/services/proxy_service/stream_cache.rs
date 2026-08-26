//! Streaming blob cache for proxied registries.
//!
//! When `registry_proxies.stream` is enabled, a proxied manifest is served as
//! soon as the manifest document itself is fetched; layers are downloaded on
//! demand. Each blob GET streams bytes to the client *while* the download
//! into the local cache is still in progress, and concurrent requests for the
//! same digest attach to a single upstream download instead of each opening
//! their own connection.
//!
//! Mechanics: the download task writes the upstream stream into a temp file
//! under `uploads/` and publishes progress through a `tokio::sync::watch`
//! channel. Readers tail the growing file, waking on every progress update.
//! On completion the temp file is digest-verified and renamed into the blob
//! store (readers that already hold an open fd keep reading the same inode
//! across the rename), the blob row is inserted into the DB, and the inflight
//! entry is dropped.

use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use ::oci_client::Reference;
use ::oci_client::secrets::RegistryAuth;
use bytes::Bytes;
use futures::StreamExt;
use sha2::Digest as _;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;

use crate::file_storage::FileStorage;
use crate::repositories::Repositories;

/// How much a tailing reader hands out per chunk at most.
const READ_CHUNK_MAX: u64 = 512 * 1024;

#[derive(Clone, Debug, Default)]
pub struct DownloadProgress {
    /// Bytes durably written to the temp file so far.
    pub written: u64,
    /// Upstream `Content-Length`, once the response headers arrived.
    pub expected: Option<u64>,
    /// Set exactly once when the download finishes: final size or an error.
    pub terminal: Option<Result<u64, String>>,
}

struct InflightEntry {
    temp_path: PathBuf,
    final_path: PathBuf,
    rx: watch::Receiver<DownloadProgress>,
}

/// Registry of in-progress proxied blob downloads, keyed by digest.
pub struct InflightBlobs {
    map: tokio::sync::Mutex<HashMap<String, Arc<InflightEntry>>>,
    storage: Arc<FileStorage>,
    repos: Arc<Repositories>,
}

#[derive(thiserror::Error, Debug)]
pub enum StreamCacheError {
    #[error("Upstream blob request failed: {0}")]
    Upstream(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Download failed: {0}")]
    Download(String),
}

impl std::fmt::Debug for InflightBlobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightBlobs").finish_non_exhaustive()
    }
}

impl InflightBlobs {
    pub fn new(storage: Arc<FileStorage>, repos: Arc<Repositories>) -> Self {
        Self {
            map: tokio::sync::Mutex::new(HashMap::new()),
            storage,
            repos,
        }
    }

    /// Attach to (or start) the download of `digest` from `image`'s registry.
    /// Returns the blob size and an `AsyncRead` that streams the blob while it
    /// downloads. `local_repo_name` gets the repo↔blob association on success.
    pub async fn fetch_or_attach(
        self: &Arc<Self>,
        client: ::oci_client::Client,
        auth: RegistryAuth,
        image: &Reference,
        digest: &str,
        local_repo_name: &str,
    ) -> Result<(u64, impl tokio::io::AsyncRead + Send + use<>), StreamCacheError> {
        let entry = self
            .entry_for(client, auth, image, digest, local_repo_name)
            .await?;

        // Wait until the size is known (headers arrived) or the download
        // finished — the HTTP response needs a Content-Length up front.
        let mut rx = entry.rx.clone();
        let size = loop {
            let p = rx.borrow().clone();
            if let Some(t) = p.terminal {
                match t {
                    Ok(sz) => break sz,
                    Err(e) => return Err(StreamCacheError::Download(e)),
                }
            }
            if let Some(sz) = p.expected {
                break sz;
            }
            if rx.changed().await.is_err() {
                return Err(StreamCacheError::Download(
                    "download task dropped".to_string(),
                ));
            }
        };

        let reader = tailing_reader(entry);
        Ok((size, reader))
    }

    /// Start the download of `digest` without consuming it (cache warming).
    pub async fn prefetch(
        self: &Arc<Self>,
        client: ::oci_client::Client,
        auth: RegistryAuth,
        image: &Reference,
        digest: &str,
        local_repo_name: &str,
    ) -> Result<(), StreamCacheError> {
        self.entry_for(client, auth, image, digest, local_repo_name)
            .await?;
        Ok(())
    }

    async fn entry_for(
        self: &Arc<Self>,
        client: ::oci_client::Client,
        auth: RegistryAuth,
        image: &Reference,
        digest: &str,
        local_repo_name: &str,
    ) -> Result<Arc<InflightEntry>, StreamCacheError> {
        let mut map = self.map.lock().await;
        if let Some(entry) = map.get(digest) {
            return Ok(entry.clone());
        }

        let temp_path = self.storage.streaming_tmp_path(digest);
        let final_path = self.storage.blob_path(digest);

        // Lost race against a finalized download: serve from the store.
        if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
            let size = tokio::fs::metadata(&final_path).await?.len();
            let (tx, rx) = watch::channel(DownloadProgress {
                written: size,
                expected: Some(size),
                terminal: Some(Ok(size)),
            });
            drop(tx);
            return Ok(Arc::new(InflightEntry {
                temp_path,
                final_path,
                rx,
            }));
        }

        // Create (truncate any stale leftover) before registering, so a
        // reader can always open the file.
        tokio::fs::File::create(&temp_path).await?;

        let (tx, rx) = watch::channel(DownloadProgress::default());
        let entry = Arc::new(InflightEntry {
            temp_path: temp_path.clone(),
            final_path: final_path.clone(),
            rx,
        });
        map.insert(digest.to_string(), entry.clone());
        drop(map);

        let this = self.clone();
        let image = image.clone();
        let digest_owned = digest.to_string();
        let repo_owned = local_repo_name.to_string();
        tokio::spawn(async move {
            let res = this
                .run_download(&client, &auth, &image, &digest_owned, &repo_owned, &tx)
                .await;
            match res {
                Ok(size) => {
                    tx.send_modify(|p| p.terminal = Some(Ok(size)));
                }
                Err(e) => {
                    tracing::warn!(digest = %digest_owned, "Streaming blob download failed: {e}");
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    tx.send_modify(|p| p.terminal = Some(Err(e.to_string())));
                }
            }
            this.map.lock().await.remove(&digest_owned);
        });

        Ok(entry)
    }

    async fn run_download(
        &self,
        client: &::oci_client::Client,
        auth: &RegistryAuth,
        image: &Reference,
        digest: &str,
        local_repo_name: &str,
        tx: &watch::Sender<DownloadProgress>,
    ) -> Result<u64, StreamCacheError> {
        // Seed credentials: `pull_blob_stream` negotiates tokens through the
        // client's auth store (`apply_auth`), which only knows registries it
        // was given credentials for.
        client
            .store_auth_if_needed(image.resolve_registry(), auth)
            .await;
        let sized = client
            .pull_blob_stream(image, digest)
            .await
            .map_err(|e| StreamCacheError::Upstream(e.to_string()))?;
        if let Some(len) = sized.content_length {
            tx.send_modify(|p| p.expected = Some(len));
        }

        let temp_path = self.storage.streaming_tmp_path(digest);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .await?;
        let mut hasher = sha2::Sha256::new();
        let mut written: u64 = 0;
        let mut stream = sized.stream;
        while let Some(chunk) = stream.next().await {
            let chunk: Bytes = chunk.map_err(|e| StreamCacheError::Upstream(e.to_string()))?;
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
            file.flush().await?;
            written += chunk.len() as u64;
            tx.send_modify(|p| p.written = written);
        }
        file.sync_data().await?;
        drop(file);

        let computed = format!("sha256:{}", hex::encode(hasher.finalize()));
        if digest.starts_with("sha256:") && computed != digest {
            return Err(StreamCacheError::Download(format!(
                "digest mismatch: expected {digest}, got {computed}"
            )));
        }

        self.storage
            .promote_temp_blob(&temp_path, digest)
            .await
            .map_err(|e| StreamCacheError::Download(e.to_string()))?;
        self.repos
            .blob
            .insert_or_ignore(digest, written as i64)
            .await
            .map_err(|e| StreamCacheError::Download(e.to_string()))?;
        self.repos
            .repo_blob_assoc
            .insert_blob_assoc_safe(local_repo_name, digest)
            .await
            .map_err(|e| StreamCacheError::Download(e.to_string()))?;
        Ok(written)
    }
}

/// An `AsyncRead` that follows a file while a download task appends to it.
/// Opens the temp file lazily (falling back to the finalized blob path — the
/// rename can win the race) and waits on the watch channel when it catches up
/// with the writer.
fn tailing_reader(entry: Arc<InflightEntry>) -> impl tokio::io::AsyncRead + Send + use<> {
    let rx = entry.rx.clone();
    let stream = futures::stream::unfold(
        (entry, rx, None::<tokio::fs::File>, 0u64),
        |(entry, mut rx, mut file, mut pos)| async move {
            loop {
                let p = rx.borrow().clone();
                let available = p.written.saturating_sub(pos);
                let finished = match &p.terminal {
                    Some(Ok(size)) => {
                        if pos >= *size {
                            return None; // EOF
                        }
                        true
                    }
                    Some(Err(e)) => {
                        let err = std::io::Error::other(format!("blob download failed: {e}"));
                        return Some((Err(err), (entry, rx, file, pos)));
                    }
                    None => false,
                };

                // On a finished download the temp file may already be renamed;
                // trust the terminal size over the last published `written`.
                let readable = if finished {
                    match &p.terminal {
                        Some(Ok(size)) => size.saturating_sub(pos),
                        _ => available,
                    }
                } else {
                    available
                };

                if readable == 0 {
                    if rx.changed().await.is_err() {
                        // Writer gone without terminal state: treat as error.
                        let err = std::io::Error::other("blob download task dropped");
                        return Some((Err(err), (entry, rx, file, pos)));
                    }
                    continue;
                }

                if file.is_none() {
                    let f = match tokio::fs::File::open(&entry.temp_path).await {
                        Ok(f) => f,
                        Err(_) => match tokio::fs::File::open(&entry.final_path).await {
                            Ok(f) => f,
                            Err(e) => return Some((Err(e), (entry, rx, file, pos))),
                        },
                    };
                    file = Some(f);
                }
                let f = file.as_mut().unwrap();
                if let Err(e) = f.seek(SeekFrom::Start(pos)).await {
                    return Some((Err(e), (entry, rx, file, pos)));
                }
                let to_read = readable.min(READ_CHUNK_MAX) as usize;
                let mut buf = vec![0u8; to_read];
                match f.read(&mut buf).await {
                    Ok(0) => {
                        // File shorter than advertised (should not happen);
                        // wait for more data or terminal state.
                        if rx.changed().await.is_err() {
                            let err = std::io::Error::other("blob download task dropped");
                            return Some((Err(err), (entry, rx, file, pos)));
                        }
                        continue;
                    }
                    Ok(n) => {
                        buf.truncate(n);
                        pos += n as u64;
                        return Some((Ok(Bytes::from(buf)), (entry, rx, file, pos)));
                    }
                    Err(e) => return Some((Err(e), (entry, rx, file, pos))),
                }
            }
        },
    );
    tokio_util::io::StreamReader::new(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::AsyncReadExt;
    use tokio::sync::watch;

    use super::*;

    fn make_entry(
        dir: &std::path::Path,
        name: &str,
    ) -> (Arc<InflightEntry>, watch::Sender<DownloadProgress>) {
        let temp_path = dir.join(format!("streaming-{name}"));
        let final_path = dir.join(name);
        let (tx, rx) = watch::channel(DownloadProgress::default());
        (
            Arc::new(InflightEntry {
                temp_path,
                final_path,
                rx,
            }),
            tx,
        )
    }

    #[tokio::test]
    async fn tailing_reader_streams_while_writer_appends() {
        let dir = test_temp_dir::test_temp_dir!();
        let dir_path = dir.as_path_untracked().to_owned();
        let (entry, tx) = make_entry(&dir_path, "blob1");
        std::fs::write(&entry.temp_path, b"").unwrap();

        let mut reader = tailing_reader(entry.clone());
        let read_task = tokio::spawn(async move {
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await.unwrap();
            out
        });

        // Append in two chunks with progress updates, then finish.
        let path = entry.temp_path.clone();
        tokio::spawn(async move {
            use std::io::Write;
            {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap();
                f.write_all(b"hello ").unwrap();
            }
            tx.send_modify(|p| p.written = 6);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap();
                f.write_all(b"world").unwrap();
            }
            tx.send_modify(|p| {
                p.written = 11;
                p.terminal = Some(Ok(11));
            });
        });

        let out = read_task.await.unwrap();
        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn tailing_reader_surfaces_download_failure() {
        let dir = test_temp_dir::test_temp_dir!();
        let dir_path = dir.as_path_untracked().to_owned();
        let (entry, tx) = make_entry(&dir_path, "blob2");
        std::fs::write(&entry.temp_path, b"par").unwrap();
        tx.send_modify(|p| p.written = 3);

        let mut reader = tailing_reader(entry.clone());
        let read_task = tokio::spawn(async move {
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send_modify(|p| p.terminal = Some(Err("boom".to_string())));

        let res = read_task.await.unwrap();
        assert!(res.is_err(), "reader must fail when the download fails");
    }

    #[tokio::test]
    async fn tailing_reader_reads_across_rename() {
        let dir = test_temp_dir::test_temp_dir!();
        let dir_path = dir.as_path_untracked().to_owned();
        let (entry, tx) = make_entry(&dir_path, "blob3");
        std::fs::write(&entry.temp_path, b"full contents").unwrap();
        // Finalize before the reader ever opens the file: rename temp → final.
        std::fs::rename(&entry.temp_path, &entry.final_path).unwrap();
        tx.send_modify(|p| {
            p.written = 13;
            p.terminal = Some(Ok(13));
        });

        let mut reader = tailing_reader(entry.clone());
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"full contents");
    }
}
