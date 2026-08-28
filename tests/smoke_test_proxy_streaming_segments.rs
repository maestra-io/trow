#![cfg(test)]

//! Segmented / stall-resilient proxied blob downloads
//! (`registry_proxies.stream = true`).
//!
//! A blob body arrives over one TCP connection, and on a long-RTT path a
//! connection that lands on a degraded route stays degraded for the whole
//! transfer. Two mitigations are covered here: splitting a large blob across
//! several ranged connections, and redialling a connection that ends short.
//!
//! Local mock registry, no network.

mod common;

mod proxy_streaming_segments {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::body::Body;
    use axum::extract::{Path as AxumPath, State};
    use axum::http::{HeaderMap, StatusCode, header};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use hyper::Request;
    use test_temp_dir::test_temp_dir;
    use tokio::net::TcpListener;
    use tower::ServiceExt;
    use trow::configuration::{RegistryProxiesConfig, SingleRegistryProxyConfig};

    use crate::common;
    use crate::common::trow_router;

    /// Over `PARALLEL_MIN_SIZE` (32 MiB), so the download is planned as
    /// several segments. Deterministic, non-uniform content so a
    /// mis-assembled file cannot pass by accident.
    fn big_layer() -> Arc<Vec<u8>> {
        static BODY: std::sync::OnceLock<Arc<Vec<u8>>> = std::sync::OnceLock::new();
        BODY.get_or_init(|| {
            let n = 40 * 1024 * 1024;
            let mut v = Vec::with_capacity(n);
            let mut x: u32 = 0x9e37_79b9;
            while v.len() < n {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                v.extend_from_slice(&x.to_le_bytes());
            }
            v.truncate(n);
            Arc::new(v)
        })
        .clone()
    }

    /// Under `PARALLEL_MIN_SIZE`, so it is fetched as a single segment: used
    /// to exercise redialling without segmentation in the picture.
    fn small_layer() -> Arc<Vec<u8>> {
        Arc::new(vec![7u8; 1024 * 1024])
    }

    fn sha256_of(bytes: &[u8]) -> String {
        format!(
            "sha256:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(bytes))
        )
    }

    fn config_digest() -> String {
        sha256_of(b"{}")
    }

    #[derive(Clone)]
    struct MockUpstream {
        body: Arc<Vec<u8>>,
        /// Every GET of the layer blob, ranged or not.
        layer_requests: Arc<AtomicUsize>,
        /// Ranges served, excluding the one-byte support probe.
        ranges: Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
        /// false = answer every request with the whole blob, as a registry
        /// that does not implement Range does.
        honour_range: bool,
        /// Number of remaining responses to cut short at `truncate_at`
        /// bytes while still advertising the full length.
        truncations: Arc<AtomicUsize>,
        truncate_at: usize,
    }

    fn manifest_json(body: &[u8]) -> String {
        format!(
            r#"{{
              "schemaVersion": 2,
              "mediaType": "application/vnd.oci.image.manifest.v1+json",
              "config": {{
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "{}",
                "size": 2
              }},
              "layers": [{{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "{}",
                "size": {}
              }}]
            }}"#,
            config_digest(),
            sha256_of(body),
            body.len()
        )
    }

    async fn serve_manifest(State(state): State<MockUpstream>) -> impl IntoResponse {
        let body = manifest_json(&state.body);
        let digest = sha256_of(body.as_bytes());
        (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json".to_string(),
                ),
                (
                    header::HeaderName::from_static("docker-content-digest"),
                    digest,
                ),
            ],
            body,
        )
    }

    /// `bytes=<start>-<end>` (end inclusive) or `bytes=<start>-`.
    fn parse_range(headers: &HeaderMap, total: usize) -> Option<(usize, usize)> {
        let raw = headers.get(header::RANGE)?.to_str().ok()?;
        let spec = raw.strip_prefix("bytes=")?;
        let (a, b) = spec.split_once('-')?;
        let start: usize = a.parse().ok()?;
        let end = if b.is_empty() {
            total - 1
        } else {
            b.parse::<usize>().ok()?.min(total - 1)
        };
        if start > end {
            None
        } else {
            Some((start, end))
        }
    }

    fn maybe_truncate(state: &MockUpstream, slice: Vec<u8>) -> (Vec<u8>, usize) {
        let declared = slice.len();
        let cut = state
            .truncations
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 { Some(n - 1) } else { None }
            })
            .is_ok();
        if cut && slice.len() > state.truncate_at {
            (slice[..state.truncate_at].to_vec(), declared)
        } else {
            (slice, declared)
        }
    }

    /// Body sent through a stream so the declared Content-Length is ours and
    /// a truncated response really is short on the wire.
    fn body_with_len(bytes: Vec<u8>, declared: usize) -> Body {
        let _ = declared;
        Body::from_stream(futures::stream::once(async move {
            Ok::<_, std::io::Error>(bytes)
        }))
    }

    async fn serve_blob_nested(
        state: State<MockUpstream>,
        headers: HeaderMap,
        AxumPath((_a, repo, digest)): AxumPath<(String, String, String)>,
    ) -> Response {
        serve_blob(state, headers, AxumPath((repo, digest))).await
    }

    async fn serve_blob(
        State(state): State<MockUpstream>,
        headers: HeaderMap,
        AxumPath((_repo, digest)): AxumPath<(String, String)>,
    ) -> Response {
        if digest != sha256_of(&state.body) {
            return (StatusCode::OK, b"{}".to_vec()).into_response();
        }
        state.layer_requests.fetch_add(1, Ordering::SeqCst);
        let total = state.body.len();
        let range = parse_range(&headers, total);

        if let (true, Some((start, end))) = (state.honour_range, range) {
            // The one-byte probe asks `bytes=0-0`; it is support detection,
            // not a segment, so it must not count as parallelism.
            if end > start {
                state.ranges.lock().unwrap().push((start, end));
            }
            let (bytes, declared) = maybe_truncate(&state, state.body[start..=end].to_vec());
            return (
                StatusCode::PARTIAL_CONTENT,
                [
                    (header::CONTENT_LENGTH, declared.to_string()),
                    (
                        header::CONTENT_RANGE,
                        format!("bytes {start}-{end}/{total}"),
                    ),
                ],
                body_with_len(bytes, declared),
            )
                .into_response();
        }

        let (bytes, declared) = maybe_truncate(&state, state.body.as_ref().clone());
        (
            StatusCode::OK,
            [(header::CONTENT_LENGTH, declared.to_string())],
            body_with_len(bytes, declared),
        )
            .into_response()
    }

    struct Upstream {
        addr: SocketAddr,
        layer_requests: Arc<AtomicUsize>,
        ranges: Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
    }

    async fn start_mock_upstream(
        body: Arc<Vec<u8>>,
        honour_range: bool,
        truncations: usize,
        truncate_at: usize,
    ) -> Upstream {
        let layer_requests = Arc::new(AtomicUsize::new(0));
        let ranges: Arc<std::sync::Mutex<Vec<(usize, usize)>>> = Default::default();
        let state = MockUpstream {
            body,
            layer_requests: layer_requests.clone(),
            ranges: ranges.clone(),
            honour_range,
            truncations: Arc::new(AtomicUsize::new(truncations)),
            truncate_at,
        };
        let app = Router::new()
            .route("/v2/", get(|| async { StatusCode::OK }))
            .route(
                "/v2/{repo}/manifests/{reference}",
                get(serve_manifest).head(serve_manifest),
            )
            .route(
                "/v2/{a}/{repo}/manifests/{reference}",
                get(serve_manifest).head(serve_manifest),
            )
            .route("/v2/{repo}/blobs/{digest}", get(serve_blob))
            .route("/v2/{a}/{repo}/blobs/{digest}", get(serve_blob_nested))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Upstream {
            addr,
            layer_requests,
            ranges,
        }
    }

    async fn start_trow(data_dir: &Path, upstream: SocketAddr) -> Router {
        let config_file = trow::configuration::ConfigFile {
            registry_proxies: RegistryProxiesConfig {
                offline: false,
                stream: true,
                registries: vec![SingleRegistryProxyConfig {
                    host: upstream.to_string(),
                    insecure: true,
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            },
            ..Default::default()
        };
        trow_router(data_dir, |cfg| {
            cfg.config_file = config_file;
        })
        .await
        .1
    }

    async fn pull_layer(trow: &Router, repo: &str, digest: &str) -> Vec<u8> {
        let resp = trow
            .clone()
            .oneshot(
                Request::get(format!("/v2/{repo}/blobs/{digest}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        common::response_body_vec(resp).await
    }

    async fn warm_manifest(trow: &Router, repo: &str) {
        let status = trow
            .clone()
            .oneshot(
                Request::get(format!("/v2/{repo}/manifests/1.0.0"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::OK);
    }

    /// A blob over the parallel threshold is fetched over several ranged
    /// connections, and the reassembled bytes are exactly the upstream's.
    /// The digest check inside trow is what makes this strict: a segment
    /// written at the wrong offset fails the pull outright.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn large_blob_is_fetched_in_parallel_segments() {
        let tmp_dir = test_temp_dir!();
        let body = big_layer();
        let up = start_mock_upstream(body.clone(), true, 0, 0).await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        warm_manifest(&trow, &repo).await;
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert_eq!(got.len(), body.len(), "streamed blob length must match");
        assert!(
            got == *body,
            "streamed blob body must match upstream byte for byte"
        );

        // 40 MiB against a 32 MiB segment target plans two segments. Segment 0
        // rides the stream that was already opened to learn the blob size, so
        // exactly one further connection is expected — and it must ask for the
        // TAIL, which is what proves a second part of the blob was being
        // fetched over its own connection rather than the whole thing again.
        let ranges = up.ranges.lock().unwrap().clone();
        assert!(
            ranges
                .iter()
                .any(|&(start, end)| start > 0 && end == body.len() - 1),
            "expected a ranged request for the tail segment, saw {ranges:?}"
        );
        assert!(
            ranges
                .iter()
                .all(|&(start, end)| end - start + 1 < body.len()),
            "a segment asked for the whole blob, so it was not really split: {ranges:?}"
        );
    }

    /// A registry that ignores `Range` must be detected by the probe and fall
    /// back to one plain stream — NOT answered with N segments each streaming
    /// the whole blob from byte 0.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn range_unsupported_upstream_falls_back_to_one_stream() {
        let tmp_dir = test_temp_dir!();
        let body = big_layer();
        let up = start_mock_upstream(body.clone(), false, 0, 0).await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        warm_manifest(&trow, &repo).await;
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert!(
            got == *body,
            "fallback path must still serve the exact blob"
        );
        let fetches = up.layer_requests.load(Ordering::SeqCst);
        assert!(
            fetches <= 3,
            "range-less upstream must not be hit once per segment; \
             expected the probe plus one stream, saw {fetches} layer GETs"
        );
    }

    /// An upstream connection that ends short is resumed from where it
    /// stopped rather than failing the pull or restarting from zero.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn short_upstream_response_is_resumed() {
        let tmp_dir = test_temp_dir!();
        let body = small_layer();
        // One truncated response, cut at 40% of the blob.
        let up = start_mock_upstream(body.clone(), true, 1, 400 * 1024).await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        warm_manifest(&trow, &repo).await;
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert_eq!(got.len(), body.len(), "resumed blob length must match");
        assert!(got == *body, "resumed blob body must match upstream");
        let fetches = up.layer_requests.load(Ordering::SeqCst);
        assert!(
            fetches >= 2,
            "expected a redial after the short response, saw {fetches} layer GETs"
        );
    }
}
