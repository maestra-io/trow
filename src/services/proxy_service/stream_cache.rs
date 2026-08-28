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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ::oci_client::Reference;
use ::oci_client::client::{BlobResponse, SizedStream};
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

    /// Fetch `digest` into the temp file and promote it into the blob store.
    ///
    /// The download is split into segments fetched concurrently over separate
    /// connections, and any segment that stalls is redialled from its current
    /// offset. Both exist for the same reason: a blob arrives over one TCP
    /// connection, and on a long-RTT path a single connection that lands on a
    /// degraded route stays degraded for the life of the transfer. See
    /// `run_segment`.
    async fn run_download(
        &self,
        client: &::oci_client::Client,
        auth: &RegistryAuth,
        image: &Reference,
        digest: &str,
        local_repo_name: &str,
        tx: &watch::Sender<DownloadProgress>,
    ) -> Result<u64, StreamCacheError> {
        // Seed credentials: the blob calls negotiate tokens through the
        // client's auth store (`apply_auth`), which only knows registries it
        // was given credentials for.
        client
            .store_auth_if_needed(image.resolve_registry(), auth)
            .await;

        // The unranged GET is also how the total size is learned — a ranged
        // response carries only its own length, and oci-client does not
        // surface `Content-Range`. Segment 0 consumes this stream rather than
        // opening a second connection for bytes it is already being sent.
        let seed = client
            .pull_blob_stream(image, digest)
            .await
            .map_err(|e| StreamCacheError::Upstream(e.to_string()))?;
        let total = seed.content_length;
        if let Some(len) = total {
            tx.send_modify(|p| p.expected = Some(len));
        }

        let plan = match total {
            Some(t) if t > PARALLEL_MIN_SIZE && range_supported(client, image, digest).await => {
                plan_segments(t)
            }
            Some(t) => vec![Segment { start: 0, end: t }],
            // No Content-Length: one segment of unknown extent. Stall
            // detection still applies; parallelism cannot.
            None => vec![Segment {
                start: 0,
                end: u64::MAX,
            }],
        };

        let temp_path = self.storage.streaming_tmp_path(digest);
        let state = DownloadState::new(plan);
        if state.plan.len() > 1 {
            tracing::debug!(
                digest,
                segments = state.plan.len(),
                "Fetching blob in parallel segments"
            );
        }

        let mut seed = Some(seed);
        let mut segments = Vec::with_capacity(state.plan.len());
        for idx in 0..state.plan.len() {
            // Only segment 0 starts at byte 0, so only it can use the stream
            // that is already open.
            let seed = if idx == 0 { seed.take() } else { None };
            segments.push(run_segment(
                client, image, digest, &temp_path, &state, tx, idx, seed,
            ));
        }
        // try_join_all drops the remaining segments on the first error, which
        // is what we want: a blob that cannot be completed must not be
        // promoted, and the siblings' connections should go away with it.
        futures::future::try_join_all(segments).await?;

        let written = state.contiguous();
        if let Some(t) = total
            && written != t
        {
            return Err(StreamCacheError::Download(format!(
                "short download: expected {t} bytes, got {written}"
            )));
        }

        // Digest verification happens over the finished file, not inline over
        // the stream: neither a resumed nor an out-of-order segment can feed a
        // sequential hasher. This is the same shape as the non-streaming path
        // (`FileStorage::write_blob_stream` -> `TemporaryFile::digest`).
        let computed = sha256_file(&temp_path).await?;
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

// ---------------------------------------------------------------------------
// Segmented, stall-resilient upstream fetch
// ---------------------------------------------------------------------------
//
// Why this is not just "open the blob and copy it": a blob body arrives over a
// single TCP connection, and on a long-RTT path a connection that lands on a
// degraded route stays degraded for the whole transfer — there is no mechanism
// by which it recovers. Measured on us-omega 2026-08-28: of ten fresh flows
// from the cache's node to S3 eu-central-1, seven ran at 15-17 MB/s and three
// at 0.17-0.37 MB/s, and trow's long-lived connections had settled on the slow
// ones. A 236 MiB image took 10m10s while a plain `curl` of the same blob from
// the same node ran at 20 MB/s. Two independent mitigations, both here:
//
//   * splitting the blob across several connections means one bad route costs
//     a fraction of the transfer rather than all of it, and
//   * a segment that stops making progress is redialled from its offset, which
//     rehashes it onto a different route.
//
// Neither helps a uniformly slow upstream, and neither is a substitute for
// fixing the route — they bound the damage.

/// Blobs at or under this size are fetched over one connection: splitting them
/// saves little absolute time and costs an extra probe round-trip.
const PARALLEL_MIN_SIZE: u64 = 32 * 1024 * 1024;
/// Bytes per segment once a blob is split.
const SEGMENT_TARGET: u64 = 32 * 1024 * 1024;
/// Upper bound on concurrent upstream connections for a single blob. Layers
/// are already fetched concurrently (containerd pulls several at once, and the
/// manifest prefetch starts them all), so this multiplies.
const MAX_SEGMENTS: usize = 8;
/// A segment that moves fewer than `STALL_MIN_BYTES` within this window — or
/// delivers no chunk at all in it — is treated as stuck.
const STALL_WINDOW: Duration = Duration::from_secs(8);
/// 1 MiB/s over `STALL_WINDOW`. Set below any plausible healthy rate and well
/// above zero: the point is to catch a collapsed route, not to police a
/// merely slow one, and every redial costs a round-trip and a fresh TLS
/// handshake.
const STALL_MIN_BYTES: u64 = 8 * 1024 * 1024;
/// Attempts per segment, initial connection included.
const MAX_SEGMENT_ATTEMPTS: usize = 4;
/// Bytes a segment buffers before it flushes and republishes progress. Also
/// what bounds a tailing reader's latency behind the writer.
const PUBLISH_INTERVAL: u64 = 1024 * 1024;

/// Half-open byte range `[start, end)` of a blob.
#[derive(Clone, Copy, Debug)]
struct Segment {
    start: u64,
    end: u64,
}

struct DownloadState {
    plan: Vec<Segment>,
    /// Bytes written by each segment, indexed alongside `plan`.
    written: Vec<AtomicU64>,
}

impl DownloadState {
    fn new(plan: Vec<Segment>) -> Self {
        let written = plan.iter().map(|_| AtomicU64::new(0)).collect();
        Self { plan, written }
    }

    /// Bytes readable from byte 0 without hitting a hole.
    ///
    /// NOT the sum of all segments: segments complete out of order, so a
    /// finished tail contributes nothing until everything before it has
    /// landed. Readers tail the file against this number, so publishing the
    /// sum would hand them sparse zeroes.
    fn contiguous(&self) -> u64 {
        let mut prefix = 0;
        for (i, seg) in self.plan.iter().enumerate() {
            let done = self.written[i].load(Ordering::Relaxed);
            prefix = seg.start + done;
            if seg.start + done < seg.end {
                break;
            }
        }
        prefix
    }
}

fn plan_segments(total: u64) -> Vec<Segment> {
    let n = total.div_ceil(SEGMENT_TARGET).clamp(1, MAX_SEGMENTS as u64);
    let per = total.div_ceil(n);
    (0..n)
        .map(|i| Segment {
            start: i * per,
            end: ((i + 1) * per).min(total),
        })
        .filter(|s| s.start < s.end)
        .collect()
}

/// One 1-byte ranged GET: does this upstream honour `Range` for this blob?
///
/// Asked before planning rather than discovered mid-download. A registry that
/// ignores `Range` answers every segment with the whole blob from byte 0, so
/// finding out late would mean N connections each streaming the full body.
async fn range_supported(client: &::oci_client::Client, image: &Reference, digest: &str) -> bool {
    match client
        .pull_blob_stream_partial(image, digest, 0, Some(1))
        .await
    {
        Ok(BlobResponse::Partial(_)) => true,
        Ok(BlobResponse::Full(_)) => false,
        Err(e) => {
            tracing::debug!(digest, "Range probe failed, using a single stream: {e}");
            false
        }
    }
}

/// Fetch one segment into `temp_path`, redialling on a stall.
///
/// `seed` is an already-open stream positioned at byte 0 and is only ever
/// passed to segment 0.
#[allow(clippy::too_many_arguments)]
async fn run_segment(
    client: &::oci_client::Client,
    image: &Reference,
    digest: &str,
    temp_path: &std::path::Path,
    state: &DownloadState,
    tx: &watch::Sender<DownloadProgress>,
    idx: usize,
    seed: Option<SizedStream>,
) -> Result<(), StreamCacheError> {
    let seg = state.plan[idx];
    // No `truncate`: the file was created by `entry_for`, and the other
    // segments are writing into it concurrently. Each segment owns its own
    // handle so they do not share a cursor.
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(temp_path)
        .await?;

    let mut seed = seed;
    let mut last_err: Option<String> = None;

    for attempt in 0..MAX_SEGMENT_ATTEMPTS {
        let pos = seg.start + state.written[idx].load(Ordering::Relaxed);
        if pos >= seg.end {
            return Ok(());
        }
        file.seek(SeekFrom::Start(pos)).await?;

        // `skip` covers the one case where the body does not start where we
        // asked: an upstream that ignored `Range` and replied from byte 0.
        let (mut stream, mut skip) = match seed.take() {
            Some(s) => (s.stream, 0u64),
            None => {
                let len = if seg.end == u64::MAX {
                    None
                } else {
                    Some(seg.end - pos)
                };
                match client
                    .pull_blob_stream_partial(image, digest, pos, len)
                    .await
                {
                    Ok(BlobResponse::Partial(s)) => (s.stream, 0),
                    Ok(BlobResponse::Full(s)) => (s.stream, pos),
                    Err(e) => {
                        last_err = Some(e.to_string());
                        continue;
                    }
                }
            }
        };

        let mut window_start = Instant::now();
        let mut window_bytes = 0u64;
        let mut unflushed = 0u64;
        let mut stalled = false;

        loop {
            // The timeout is the "delivers nothing at all" arm of stall
            // detection; the byte counter below is the "delivers a trickle"
            // arm. A dead connection hits neither without this.
            let chunk = match tokio::time::timeout(STALL_WINDOW, stream.next()).await {
                Err(_) => {
                    stalled = true;
                    break;
                }
                Ok(None) => break,
                Ok(Some(Ok(c))) => c,
                Ok(Some(Err(e))) => {
                    last_err = Some(e.to_string());
                    stalled = true;
                    break;
                }
            };

            let mut chunk: Bytes = chunk;
            if skip > 0 {
                let n = skip.min(chunk.len() as u64);
                chunk = chunk.slice(n as usize..);
                skip -= n;
                if chunk.is_empty() {
                    continue;
                }
            }

            // Segment 0's seed stream, and any `Full` response, run to the end
            // of the blob. Stop at the segment boundary or segments overwrite
            // each other.
            let done = state.written[idx].load(Ordering::Relaxed);
            let room = seg.end.saturating_sub(seg.start + done);
            if room == 0 {
                break;
            }
            if chunk.len() as u64 > room {
                chunk = chunk.slice(..room as usize);
            }

            file.write_all(&chunk).await?;
            let n = chunk.len() as u64;
            state.written[idx].fetch_add(n, Ordering::Relaxed);
            unflushed += n;
            window_bytes += n;

            // Flush in batches rather than per chunk: `File::flush` awaits the
            // in-flight blocking write, so doing it per chunk serialises every
            // socket read behind a disk round-trip.
            if unflushed >= PUBLISH_INTERVAL {
                file.flush().await?;
                unflushed = 0;
                tx.send_modify(|p| p.written = state.contiguous());
            }

            if window_start.elapsed() >= STALL_WINDOW {
                if window_bytes < STALL_MIN_BYTES {
                    stalled = true;
                    break;
                }
                window_start = Instant::now();
                window_bytes = 0;
            }
        }

        file.flush().await?;
        tx.send_modify(|p| p.written = state.contiguous());

        let done = state.written[idx].load(Ordering::Relaxed);
        let complete = if seg.end == u64::MAX {
            !stalled && last_err.is_none()
        } else {
            seg.start + done >= seg.end
        };
        if complete {
            file.sync_data().await?;
            return Ok(());
        }

        tracing::info!(
            digest,
            segment = idx,
            attempt,
            offset = seg.start + done,
            "Upstream segment stalled or ended short, redialling"
        );
    }

    Err(StreamCacheError::Download(format!(
        "segment {idx} of {digest} did not complete in {MAX_SEGMENT_ATTEMPTS} attempts{}",
        last_err.map(|e| format!(": {e}")).unwrap_or_default()
    )))
}

async fn sha256_file(path: &std::path::Path) -> Result<String, StreamCacheError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
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

    fn state(plan: &[(u64, u64)], written: &[u64]) -> DownloadState {
        let st = DownloadState::new(
            plan.iter()
                .map(|&(start, end)| Segment { start, end })
                .collect(),
        );
        for (i, w) in written.iter().enumerate() {
            st.written[i].store(*w, Ordering::Relaxed);
        }
        st
    }

    #[test]
    fn plan_segments_covers_the_blob_exactly() {
        for total in [
            PARALLEL_MIN_SIZE + 1,
            SEGMENT_TARGET * 2,
            SEGMENT_TARGET * 7 + 12345,
            SEGMENT_TARGET * 64,
            u64::from(u32::MAX),
        ] {
            let plan = plan_segments(total);
            assert!(!plan.is_empty(), "total {total} produced no segments");
            assert!(
                plan.len() <= MAX_SEGMENTS,
                "total {total} produced {} segments, over the cap",
                plan.len()
            );
            assert_eq!(
                plan[0].start, 0,
                "total {total}: first segment must start at 0"
            );
            assert_eq!(
                plan.last().unwrap().end,
                total,
                "total {total}: last segment must end at the blob end"
            );
            for pair in plan.windows(2) {
                assert_eq!(
                    pair[0].end, pair[1].start,
                    "total {total}: segments must be contiguous, no gap and no overlap"
                );
            }
            let covered: u64 = plan.iter().map(|s| s.end - s.start).sum();
            assert_eq!(covered, total, "total {total}: coverage must be exact");
        }
    }

    /// The readable prefix is what tailing readers are handed. Segments land
    /// out of order, so a finished tail must contribute NOTHING until every
    /// byte before it exists — otherwise a reader is served sparse zeroes.
    #[test]
    fn contiguous_stops_at_the_first_hole() {
        let plan = [(0, 10), (10, 20), (20, 30)];

        assert_eq!(state(&plan, &[0, 0, 0]).contiguous(), 0);
        assert_eq!(state(&plan, &[4, 0, 0]).contiguous(), 4);
        // Segment 2 is complete but segment 0 is not: still 4 readable bytes.
        assert_eq!(state(&plan, &[4, 0, 10]).contiguous(), 4);
        assert_eq!(state(&plan, &[4, 10, 10]).contiguous(), 4);
        // Segment 0 completes and the prefix jumps across both finished ones.
        assert_eq!(state(&plan, &[10, 10, 10]).contiguous(), 30);
        // Middle segment partially done, tail done: prefix stops mid-segment.
        assert_eq!(state(&plan, &[10, 3, 10]).contiguous(), 13);
    }

    #[test]
    fn contiguous_handles_the_unknown_length_segment() {
        // No Content-Length: one open-ended segment, never "complete", so the
        // prefix is simply what has been written.
        let st = state(&[(0, u64::MAX)], &[777]);
        assert_eq!(st.contiguous(), 777);
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
