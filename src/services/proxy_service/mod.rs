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
            .fetch_or_attach(cl, auth, image, digest.as_str(), local_repo_name)
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
            let mut missing = Vec::new();
            for blob_digest in &blobs {
                if !self.repos.blob.exists(blob_digest).await? {
                    missing.push((*blob_digest).to_string());
                }
            }
            if !missing.is_empty() {
                let inflight = self.inflight.clone();
                let cl = cl.clone();
                let auth = auth.clone();
                let ref_ = ref_.clone();
                let repo = local_repo_name.to_string();
                tokio::spawn(async move {
                    for blob_digest in missing {
                        if let Err(e) = inflight
                            .prefetch(cl.clone(), auth.clone(), &ref_, &blob_digest, &repo)
                            .await
                        {
                            tracing::warn!(
                                digest = %blob_digest,
                                "Failed to start blob prefetch: {e}"
                            );
                        }
                    }
                });
            }
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
