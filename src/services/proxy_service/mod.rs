//! Proxy service: downloads proxied images from remote registries.

pub(crate) mod errors;
pub(crate) mod oci_client;
pub(crate) mod stream_cache;

use std::pin::Pin;
use std::sync::Arc;

use ::oci_client::Reference;
use ::oci_client::secrets::RegistryAuth;
use futures::future::try_join_all;

use self::errors::DownloadRemoteImageError;
use self::oci_client::{MIME_TYPES_DISTRIBUTION_MANIFEST, get_oci_client};
use self::stream_cache::InflightBlobs;
use crate::configuration::SingleRegistryProxyConfig;
use crate::file_storage::FileStorage;
use crate::repositories::Repositories;
use crate::services::Error;
use crate::services::blob_service::BlobReader;
use crate::utils::digest::{Digest, DigestError};
use crate::utils::manifest::OCIManifest;

#[derive(Debug)]
pub struct ProxyService {
    repos: Arc<Repositories>,
    storage: Arc<FileStorage>,
    /// `registry_proxies.stream`: serve manifests as soon as they are
    /// fetched and stream layers on demand instead of buffering the whole
    /// image before the first byte is returned.
    stream: bool,
    inflight: Arc<InflightBlobs>,
}

impl ProxyService {
    pub fn new(repos: Arc<Repositories>, storage: Arc<FileStorage>, stream: bool) -> Self {
        let inflight = Arc::new(InflightBlobs::new(storage.clone(), repos.clone()));
        Self {
            repos,
            storage,
            stream,
            inflight,
        }
    }

    pub fn stream_enabled(&self) -> bool {
        self.stream
    }

    /// Serve a proxied blob that is not (fully) cached yet: attach to the
    /// in-flight download (starting it if needed) and stream while it runs.
    pub async fn stream_blob(
        &self,
        image: &Reference,
        proxy_config: Option<&SingleRegistryProxyConfig>,
        digest: &Digest,
        local_repo_name: &str,
    ) -> Result<BlobReader<Pin<Box<dyn tokio::io::AsyncRead + Send>>>, Error> {
        let (cl, auth) = get_oci_client(image.registry(), proxy_config)
            .await
            .map_err(|e| Error::Proxy(Box::new(e)))?;
        let (size, reader) = self
            .inflight
            // No descriptor here: the manifest is not in scope on a bare blob
            // GET. In practice the download is already registered with its
            // size by `download_manifest_and_layers`, and this only starts one
            // itself when a client asks for a blob whose manifest never came
            // through this instance.
            .fetch_or_attach(cl, auth, image, digest.as_str(), local_repo_name, None)
            .await?;
        Ok(BlobReader::new_boxed(
            digest.clone(),
            size as usize,
            Box::pin(reader),
        ))
    }

    /// Returns the manifest digest that was resolved/downloaded.
    pub async fn download_image(
        &self,
        image: &Reference,
        proxy_config: Option<&SingleRegistryProxyConfig>,
    ) -> Result<String, Error> {
        let repo_name = format!("f/{}/{}", image.registry(), image.repository());
        tracing::debug!("Downloading proxied image {}", repo_name);

        let try_cl = match get_oci_client(image.registry(), proxy_config).await {
            Ok(cl) => Some(cl),
            Err(e) => {
                tracing::warn!("Could not get an OCI client: {e}");
                None
            }
        };

        let digests = self
            .collect_candidate_digests(image, &repo_name, try_cl.as_ref())
            .await?;

        for mani_digest in digests {
            let has_manifest = self
                .repos
                .repo_blob_assoc
                .manifest_exists_in_repo(&mani_digest, &repo_name)
                .await?;
            if has_manifest {
                // A cached manifest does NOT imply cached layers: GC evicts
                // blobs by LRU and leaves manifests behind, and a download can
                // have failed after the manifest was stored. Returning here
                // without registering the missing ones is how a blob GET ends
                // up with no descriptor size — one connection, unsegmented,
                // and with no sibling to compare against it never redials off
                // a degraded route either.
                //
                // Seen in production 2026-08-28: 11 of 12 layers cached, the
                // twelfth fetched over a single collapsed connection at
                // ~0.3 MB/s, 12 minutes and still going. This is the most
                // common cold-blob shape in steady state, not an edge case.
                if self.stream
                    && let Some((cl, auth)) = &try_cl
                    && let Ok(stored) = self.repos.manifest.find(&mani_digest).await
                    && let Ok(manifest) = serde_json::from_slice::<OCIManifest>(&stored.blob)
                {
                    let ref_to_dl = image.clone_with_digest(mani_digest.clone());
                    self.prefetch_missing_layers(cl, auth, &ref_to_dl, &repo_name, &manifest)
                        .await?;
                }
                return Ok(mani_digest);
            }
            if let Some((cl, auth)) = &try_cl {
                let ref_to_dl = image.clone_with_digest(mani_digest.clone());
                match self
                    .download_manifest_and_layers(cl, auth, &ref_to_dl, &repo_name)
                    .await
                {
                    Err(e) => tracing::warn!("Failed to download proxied image: {}", e),
                    Ok(()) => {
                        if let Some(tag) = image.tag() {
                            self.repos.tag.upsert(tag, &repo_name, &mani_digest).await?;
                        }
                        return Ok(mani_digest);
                    }
                }
            }
        }

        Err(Error::Proxy(Box::new(
            DownloadRemoteImageError::DownloadAttemptsFailed,
        )))
    }

    async fn collect_candidate_digests(
        &self,
        image: &Reference,
        repo_name: &str,
        cl: Option<&(::oci_client::Client, RegistryAuth)>,
    ) -> Result<Vec<String>, Error> {
        if let Some(d) = image.digest() {
            return Ok(vec![d.to_string()]);
        }
        let Some(tag) = image.tag() else {
            return Err(Error::Digest(DigestError::InvalidDigest(String::new())));
        };

        let mut digests = Vec::new();
        let local_digest = self.repos.tag.find_manifest_digest(repo_name, tag).await?;

        if let Some((cl, auth)) = cl {
            if let Ok(remote) = cl.fetch_manifest_digest(image, auth).await {
                if Some(&remote) != local_digest.as_ref() {
                    digests.push(remote);
                }
            } else {
                tracing::warn!("Failed to fetch remote tag digest");
            }
        }
        if let Some(local_digest) = local_digest {
            digests.push(local_digest);
        }
        Ok(digests)
    }

    /// Register a background download for every layer of `manifest` that the
    /// blob store does not already have, carrying the size from the layer
    /// descriptor.
    ///
    /// The size is the point. Without it a blob GET starts a download that
    /// cannot be planned — one connection, no siblings, and therefore no
    /// stall detection either, because that comparison is relative. On a path
    /// where a fraction of connections land on a degraded route, that is the
    /// difference between a 30-second pull and a 12-minute one.
    ///
    /// Registration is deliberately NOT spawned: `prefetch` only creates the
    /// inflight entry and spawns the download itself, so this costs one
    /// `exists` lookup and one file create per missing layer, and in exchange
    /// every entry carries its descriptor size before the manifest response
    /// goes out. Spawning it raced the client, and whoever lost started an
    /// unplanned single-connection download.
    ///
    /// An index/list manifest has no layers of its own and is a no-op here;
    /// its children register when they are resolved.
    async fn prefetch_missing_layers(
        &self,
        cl: &::oci_client::Client,
        auth: &RegistryAuth,
        ref_: &Reference,
        local_repo_name: &str,
        manifest: &OCIManifest,
    ) -> Result<(), Error> {
        let sizes: std::collections::HashMap<&str, u64> =
            manifest.get_local_blob_sizes().into_iter().collect();
        for blob_digest in manifest.get_local_blob_digests() {
            if self.repos.blob.exists(blob_digest).await? {
                continue;
            }
            if let Err(e) = self
                .inflight
                .prefetch(
                    cl.clone(),
                    auth.clone(),
                    ref_,
                    blob_digest,
                    local_repo_name,
                    sizes.get(blob_digest).copied(),
                )
                .await
            {
                tracing::warn!(digest = %blob_digest, "Failed to start blob prefetch: {e}");
            }
        }
        Ok(())
    }

    async fn download_manifest_and_layers(
        &self,
        cl: &::oci_client::Client,
        auth: &RegistryAuth,
        ref_: &Reference,
        local_repo_name: &str,
    ) -> Result<(), Error> {
        tracing::debug!("Downloading manifest + layers for {}", ref_);

        let (raw_manifest, digest) = cl
            .pull_manifest_raw(ref_, auth, MIME_TYPES_DISTRIBUTION_MANIFEST)
            .await
            .map_err(DownloadRemoteImageError::from)?;
        let manifest: OCIManifest =
            serde_json::from_slice(&raw_manifest).map_err(DownloadRemoteImageError::from)?;

        let blobs = manifest.get_local_blob_digests();
        if self.stream {
            // Streaming mode: store the manifest right away so the client can
            // proceed; kick off background downloads for the layers so the
            // cache warms even before the first blob GET arrives. Blob GETs
            // for not-yet-cached layers attach to these downloads.
            self.prefetch_missing_layers(cl, auth, ref_, local_repo_name, &manifest)
                .await?;
        } else {
            let futures = blobs
                .iter()
                .map(|l| self.download_blob(cl, ref_, l, local_repo_name));
            try_join_all(futures).await?;
        }

        self.repos
            .manifest
            .insert_or_ignore(&digest, &raw_manifest)
            .await?;
        self.repos
            .repo_blob_assoc
            .insert_manifest_assoc_safe(local_repo_name, &digest)
            .await?;
        Ok(())
    }

    async fn download_blob(
        &self,
        cl: &::oci_client::Client,
        ref_: &Reference,
        layer_digest: &str,
        local_repo_name: &str,
    ) -> Result<(), Error> {
        tracing::trace!("Downloading blob {}", layer_digest);
        let already_has_blob = self.repos.blob.exists(layer_digest).await?;

        if !already_has_blob {
            let stream = cl
                .pull_blob_stream(ref_, layer_digest)
                .await
                .map_err(DownloadRemoteImageError::from)?;
            let path = self
                .storage
                .write_blob_stream(layer_digest, stream, true)
                .await?;
            let size = path.metadata().map_err(|e| Error::Storage(e.into()))?.len() as i64;
            self.repos.blob.insert_or_ignore(layer_digest, size).await?;
        }
        self.repos
            .repo_blob_assoc
            .insert_blob_assoc_safe(local_repo_name, layer_digest)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ::oci_client::Reference;

    use crate::file_storage::FileStorage;
    use crate::services::proxy_service::ProxyService;
    use crate::test_utilities::{repos_in_memory, test_temp_dir};

    fn setup_service(repos: Arc<super::super::super::repositories::Repositories>) -> ProxyService {
        let dir = test_temp_dir!();
        let storage = Arc::new(FileStorage::new(dir.as_path_untracked().to_owned()).unwrap());
        ProxyService::new(repos, storage, false)
    }

    #[tokio::test]
    async fn download_image_returns_cached_manifest() {
        let repos = repos_in_memory().await;
        let svc = setup_service(repos.clone());

        let digest = "sha256:abc123def456789012345678901234567890123456789012345678901234567";
        let repo_name = "f/docker.io/library/alpine";
        let manifest_bytes: &[u8] = b"{}";

        // Insert manifest and association so proxy finds it locally
        sqlx::query!(
            "INSERT INTO manifest (digest, json, blob) VALUES (?, ?, ?)",
            digest,
            manifest_bytes,
            manifest_bytes
        )
        .execute(repos.db_rw())
        .await
        .unwrap();
        sqlx::query!(
            "INSERT INTO repo_blob_assoc (repo_name, blob_digest, manifest_digest) VALUES (?, NULL, ?)",
            repo_name, digest
        )
        .execute(repos.db_rw())
        .await
        .unwrap();

        // Use digest-based reference to skip network calls
        let image = Reference::with_digest(
            "docker.io".to_string(),
            "library/alpine".to_string(),
            digest.to_string(),
        );
        let result = svc.download_image(&image, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), digest);
    }
}
