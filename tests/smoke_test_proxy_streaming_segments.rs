#![cfg(test)]

//! Segmented / stall-resilient proxied blob downloads
//! (`registry_proxies.stream = true`).
//!
//! A blob body arrives over one TCP connection, and on a long-RTT path a
//! connection that lands on a degraded route stays degraded for the whole
//! transfer. Covered here: splitting a large blob across ranged connections,
//! redialling a connection that dies or falls far behind its siblings, and —
//! just as important — NOT redialling one that is merely slow.
//!
//! Local mock registry, no network.

mod common;

mod proxy_streaming_segments {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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

    /// Over the 32 MiB parallel threshold, so the download is planned as
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

    /// Under the threshold, so it is fetched as a single segment.
    fn small_layer() -> Arc<Vec<u8>> {
        Arc::new((0..1024u32 * 1024).map(|i| (i % 251) as u8).collect())
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

    /// Emit a body in `chunk`-sized pieces `delay` apart.
    #[derive(Clone, Copy)]
    struct Pace {
        chunk: usize,
        delay: Duration,
    }

    #[derive(Clone)]
    struct MockUpstream {
        body: Arc<Vec<u8>>,
        /// Every GET of the layer blob, ranged or not.
        layer_requests: Arc<AtomicUsize>,
        /// Ranges served, excluding the one-byte support probe.
        ranges: Arc<Mutex<Vec<(usize, usize)>>>,
        /// false = answer every request with the whole blob, as a registry
        /// that does not implement Range does.
        honour_range: bool,
        /// false = chunked response with no Content-Length, so the client
        /// cannot know the blob's length up front.
        send_content_length: bool,
        /// Responses still to be cut short at `truncate_at` bytes while
        /// advertising the full length.
        truncations: Arc<AtomicUsize>,
        truncate_at: usize,
        /// Pacing applied to every layer response.
        pace: Arc<Mutex<Option<Pace>>>,
        /// Pacing applied to a ranged request that does NOT start at 0, for
        /// this many responses. Models one segment on a degraded route.
        slow_tail: Option<Pace>,
        slow_tail_remaining: Arc<AtomicUsize>,
    }

    impl MockUpstream {
        fn new(body: Arc<Vec<u8>>) -> Self {
            Self {
                body,
                layer_requests: Default::default(),
                ranges: Default::default(),
                honour_range: true,
                send_content_length: true,
                truncations: Default::default(),
                truncate_at: 0,
                pace: Default::default(),
                slow_tail: None,
                slow_tail_remaining: Default::default(),
            }
        }
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

    /// Cut this response short, if one is still owed AND this response is
    /// actually long enough to cut. Checking the length first matters: a
    /// request too short to truncate must not silently spend the budget.
    fn maybe_truncate(state: &MockUpstream, slice: Vec<u8>) -> Vec<u8> {
        if slice.len() <= state.truncate_at {
            return slice;
        }
        let claimed = state
            .truncations
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 { Some(n - 1) } else { None }
            })
            .is_ok();
        if claimed {
            slice[..state.truncate_at].to_vec()
        } else {
            slice
        }
    }

    fn paced_body(bytes: Vec<u8>, pace: Option<Pace>) -> Body {
        match pace {
            None => Body::from_stream(futures::stream::once(async move {
                Ok::<_, std::io::Error>(bytes)
            })),
            Some(p) => {
                let chunks: Vec<Vec<u8>> =
                    bytes.chunks(p.chunk.max(1)).map(|c| c.to_vec()).collect();
                Body::from_stream(futures::stream::unfold(
                    chunks.into_iter(),
                    move |mut it| async move {
                        let c = it.next()?;
                        tokio::time::sleep(p.delay).await;
                        Some((Ok::<_, std::io::Error>(c), it))
                    },
                ))
            }
        }
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
        let base_pace = *state.pace.lock().unwrap();

        if let (true, Some((start, end))) = (state.honour_range, range) {
            // The one-byte probe asks `bytes=0-0`; it is support detection,
            // not a segment, so it must not count as parallelism.
            if end > start {
                state.ranges.lock().unwrap().push((start, end));
            }
            // A tail segment on a "degraded route": paced for the first N
            // responses, healthy afterwards.
            let pace = match state.slow_tail {
                Some(p)
                    if start > 0
                        && end > start
                        && state
                            .slow_tail_remaining
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                                if n > 0 { Some(n - 1) } else { None }
                            })
                            .is_ok() =>
                {
                    Some(p)
                }
                _ => base_pace,
            };
            let bytes = maybe_truncate(&state, state.body[start..=end].to_vec());
            let declared = end - start + 1;
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}").parse().unwrap(),
            );
            if state.send_content_length {
                headers.insert(header::CONTENT_LENGTH, declared.into());
            }
            return (
                StatusCode::PARTIAL_CONTENT,
                headers,
                paced_body(bytes, pace),
            )
                .into_response();
        }

        let bytes = maybe_truncate(&state, state.body.as_ref().clone());
        let mut headers = HeaderMap::new();
        if state.send_content_length {
            headers.insert(header::CONTENT_LENGTH, total.into());
        }
        (StatusCode::OK, headers, paced_body(bytes, base_pace)).into_response()
    }

    struct Upstream {
        addr: SocketAddr,
        layer_requests: Arc<AtomicUsize>,
        ranges: Arc<Mutex<Vec<(usize, usize)>>>,
    }

    async fn start_mock_upstream(state: MockUpstream) -> Upstream {
        let layer_requests = state.layer_requests.clone();
        let ranges = state.ranges.clone();
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
        let up = start_mock_upstream(MockUpstream::new(body.clone())).await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        warm_manifest(&trow, &repo).await;
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert_eq!(got.len(), body.len(), "streamed blob length must match");
        assert!(
            got == *body,
            "streamed blob body must match upstream byte for byte"
        );

        // 40 MiB against a 32 MiB segment target plans two segments, each
        // dialled with its own ranged request.
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
        let up = start_mock_upstream(MockUpstream {
            honour_range: false,
            ..MockUpstream::new(body.clone())
        })
        .await;
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
        let up = start_mock_upstream(MockUpstream {
            truncations: Arc::new(AtomicUsize::new(1)),
            truncate_at: 400 * 1024,
            ..MockUpstream::new(body.clone())
        })
        .await;
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

    /// The regression this whole design is shaped around: an upstream that is
    /// SLOW must still complete.
    ///
    /// Stall detection that used an absolute byte rate could not tell a bad
    /// route from a slow link, so under congestion it redialled every attempt
    /// and then failed the download — worse than the un-segmented code, which
    /// simply took a long time. Here the blob trickles well under any such
    /// floor, for longer than the sampling window, and must arrive on the
    /// FIRST connection: no redial, no failure.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn slow_upstream_completes_without_redialling() {
        let tmp_dir = test_temp_dir!();
        let body = small_layer();
        // ~1 MiB spread over ~9.6s: far below any plausible absolute floor,
        // and past the sampling window, but never idle long enough to look
        // like a dead connection.
        let up = start_mock_upstream(MockUpstream {
            pace: Arc::new(Mutex::new(Some(Pace {
                chunk: 44 * 1024,
                delay: Duration::from_millis(400),
            }))),
            ..MockUpstream::new(body.clone())
        })
        .await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        warm_manifest(&trow, &repo).await;
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert!(got == *body, "a slow upstream must still deliver the blob");
        let fetches = up.layer_requests.load(Ordering::SeqCst);
        assert_eq!(
            fetches, 1,
            "a uniformly slow upstream must not be redialled at all, saw {fetches} layer GETs"
        );
    }

    /// The case redialling exists for: one segment far slower than its
    /// sibling. The tail is paced for its first response only; the head
    /// finishes immediately and becomes the yardstick. The slow segment must
    /// be dropped and re-dialled, and the blob must still be exact.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn segment_far_slower_than_its_sibling_is_redialled() {
        let tmp_dir = test_temp_dir!();
        let body = big_layer();
        let up = start_mock_upstream(MockUpstream {
            slow_tail: Some(Pace {
                chunk: 64 * 1024,
                delay: Duration::from_millis(500),
            }),
            slow_tail_remaining: Arc::new(AtomicUsize::new(1)),
            ..MockUpstream::new(body.clone())
        })
        .await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        warm_manifest(&trow, &repo).await;
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert!(
            got == *body,
            "the blob must be exact after a segment was redialled"
        );
        let ranges = up.ranges.lock().unwrap().clone();
        let tail_dials = ranges.iter().filter(|&&(start, _)| start > 0).count();
        assert!(
            tail_dials >= 2,
            "expected the slow tail segment to be redialled, saw ranges {ranges:?}"
        );
    }

    /// No Content-Length anywhere: the descriptor is out of scope (a bare blob
    /// GET) and the upstream answers chunked. The length is then unknowable,
    /// so the download runs as one open-ended segment — and a response that
    /// ends short must still be resumed rather than accepted or failed.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn unknown_length_short_response_is_resumed() {
        let tmp_dir = test_temp_dir!();
        let body = small_layer();
        let up = start_mock_upstream(MockUpstream {
            send_content_length: false,
            truncations: Arc::new(AtomicUsize::new(1)),
            truncate_at: 400 * 1024,
            ..MockUpstream::new(body.clone())
        })
        .await;
        let trow = start_trow(tmp_dir.as_path_untracked(), up.addr).await;
        let repo = format!("f/{}/team/app", up.addr);

        // Deliberately NO manifest GET: that is what leaves the layer size
        // unknown to the proxy.
        let got = pull_layer(&trow, &repo, &sha256_of(&body)).await;

        assert_eq!(got.len(), body.len(), "resumed blob length must match");
        assert!(got == *body, "resumed blob body must match upstream");
        let fetches = up.layer_requests.load(Ordering::SeqCst);
        assert!(
            fetches >= 2,
            "expected a redial after the short chunked response, saw {fetches} layer GETs"
        );
    }
}
