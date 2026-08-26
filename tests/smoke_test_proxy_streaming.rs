#![cfg(test)]

//! Streaming pull-through tests (`registry_proxies.stream = true`).
//!
//! These exercise the behaviour that upstream trow does not have: a proxied
//! manifest is served without waiting for its layers, and blob GETs stream
//! from upstream while the cache entry is still being written.
//!
//! They run against a local mock registry (no network), so they are
//! deterministic and safe in CI.

mod common;

mod proxy_streaming {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::{Path as AxumPath, State};
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use hyper::Request;
    use test_temp_dir::test_temp_dir;
    use tokio::net::TcpListener;
    use tower::ServiceExt;
    use trow::TrowServerState;
    use trow::configuration::{RegistryProxiesConfig, SingleRegistryProxyConfig};

    use crate::common;
    use crate::common::trow_router;

    const LAYER_BODY: &[u8] = b"streaming-layer-contents";

    /// Mock upstream registry. Serves one image whose single layer trickles
    /// out in chunks, so a client that waits for the whole blob is
    /// distinguishable from one that streams.
    #[derive(Clone)]
    struct MockUpstream {
        /// Delay before the *last* chunk of the layer is emitted.
        tail_delay: Duration,
        blob_requests: Arc<AtomicUsize>,
    }

    fn layer_digest() -> String {
        format!(
            "sha256:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(LAYER_BODY))
        )
    }

    fn config_digest() -> String {
        format!(
            "sha256:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(b"{}"))
        )
    }

    fn manifest_json() -> String {
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
            layer_digest(),
            LAYER_BODY.len()
        )
    }

    async fn serve_manifest() -> impl IntoResponse {
        let body = manifest_json();
        let digest = format!(
            "sha256:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(body.as_bytes()))
        );
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

    async fn serve_blob_nested(
        state: State<MockUpstream>,
        AxumPath((_a, repo, digest)): AxumPath<(String, String, String)>,
    ) -> impl IntoResponse {
        serve_blob(state, AxumPath((repo, digest))).await
    }

    async fn serve_blob(
        State(state): State<MockUpstream>,
        AxumPath((_repo, digest)): AxumPath<(String, String)>,
    ) -> impl IntoResponse {
        state.blob_requests.fetch_add(1, Ordering::SeqCst);
        let body: Vec<u8> = if digest == layer_digest() {
            LAYER_BODY.to_vec()
        } else {
            b"{}".to_vec()
        };
        let delay = state.tail_delay;
        let len = body.len();
        // Emit the payload in two parts: head immediately, tail after a delay.
        let head = body[..len / 2].to_vec();
        let tail = body[len / 2..].to_vec();
        let stream = futures::stream::StreamExt::chain(
            futures::stream::once(async move { Ok::<_, std::io::Error>(head) }),
            futures::stream::once(async move {
                tokio::time::sleep(delay).await;
                Ok::<_, std::io::Error>(tail)
            }),
        );
        (
            StatusCode::OK,
            [(header::CONTENT_LENGTH, len.to_string())],
            Body::from_stream(stream),
        )
    }

    async fn start_mock_upstream(tail_delay: Duration) -> (SocketAddr, Arc<AtomicUsize>) {
        let blob_requests = Arc::new(AtomicUsize::new(0));
        let state = MockUpstream {
            tail_delay,
            blob_requests: blob_requests.clone(),
        };
        // `head()` matters: oci_client resolves a tag with HEAD
        // /manifests/<tag> and reads Docker-Content-Digest before pulling.
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
        (addr, blob_requests)
    }

    async fn start_trow(
        data_dir: &Path,
        upstream: SocketAddr,
        stream: bool,
    ) -> (Arc<TrowServerState>, Router) {
        let config_file = trow::configuration::ConfigFile {
            registry_proxies: RegistryProxiesConfig {
                offline: false,
                stream,
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
    }

    async fn get_manifest_status(cl: &Router, name: &str, tag: &str) -> StatusCode {
        cl.clone()
            .oneshot(
                Request::get(format!("/v2/{name}/manifests/{tag}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// The point of the feature: with `stream = true` a manifest GET must not
    /// wait for the layer download, so it returns well before the (slow)
    /// layer finishes. Without it, the same request blocks for the layer.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn manifest_returns_before_layers_finish() {
        let tmp_dir = test_temp_dir!();
        let tail_delay = Duration::from_millis(1500);
        let (addr, _) = start_mock_upstream(tail_delay).await;

        let trow = start_trow(tmp_dir.as_path_untracked(), addr, true).await.1;
        let repo = format!("f/{addr}/team/app");

        let start = std::time::Instant::now();
        let status = get_manifest_status(&trow, &repo, "1.0.0").await;
        let elapsed = start.elapsed();

        assert_eq!(status, StatusCode::OK);
        assert!(
            elapsed < tail_delay,
            "streaming manifest GET took {elapsed:?}, i.e. it waited for the layer download"
        );
    }

    /// Control: with streaming disabled (upstream behaviour) the manifest GET
    /// blocks until the layer is fully downloaded. This is what makes the
    /// test above meaningful — it fails without the feature.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn manifest_blocks_on_layers_without_streaming() {
        let tmp_dir = test_temp_dir!();
        let tail_delay = Duration::from_millis(1500);
        let (addr, _) = start_mock_upstream(tail_delay).await;

        let trow = start_trow(tmp_dir.as_path_untracked(), addr, false).await.1;
        let repo = format!("f/{addr}/team/app");

        let start = std::time::Instant::now();
        let status = get_manifest_status(&trow, &repo, "1.0.0").await;
        let elapsed = start.elapsed();

        assert_eq!(status, StatusCode::OK);
        assert!(
            elapsed >= tail_delay,
            "non-streaming manifest GET returned in {elapsed:?}, expected it to block on the layer"
        );
    }

    /// A blob GET that arrives while the layer is still downloading must
    /// serve the complete, correct payload (tailing the in-progress file).
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn blob_streams_while_download_in_flight() {
        let tmp_dir = test_temp_dir!();
        let (addr, _) = start_mock_upstream(Duration::from_millis(800)).await;

        let trow = start_trow(tmp_dir.as_path_untracked(), addr, true).await.1;
        let repo = format!("f/{addr}/team/app");

        assert_eq!(
            get_manifest_status(&trow, &repo, "1.0.0").await,
            StatusCode::OK
        );

        let resp = trow
            .clone()
            .oneshot(
                Request::get(format!("/v2/{repo}/blobs/{}", layer_digest()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = common::response_body_vec(resp).await;
        assert_eq!(body, LAYER_BODY, "streamed blob body must match upstream");
    }

    /// Concurrent GETs for the same not-yet-cached blob share ONE upstream
    /// download instead of each opening its own connection.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn concurrent_blob_gets_share_one_upstream_download() {
        let tmp_dir = test_temp_dir!();
        let (addr, blob_requests) = start_mock_upstream(Duration::from_millis(700)).await;

        let trow = start_trow(tmp_dir.as_path_untracked(), addr, true).await.1;
        let repo = format!("f/{addr}/team/app");
        assert_eq!(
            get_manifest_status(&trow, &repo, "1.0.0").await,
            StatusCode::OK
        );

        let before = blob_requests.load(Ordering::SeqCst);
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let trow = trow.clone();
            let repo = repo.clone();
            tasks.push(tokio::spawn(async move {
                let resp = trow
                    .oneshot(
                        Request::get(format!("/v2/{repo}/blobs/{}", layer_digest()))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
                common::response_body_vec(resp).await
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), LAYER_BODY);
        }
        let layer_fetches = blob_requests.load(Ordering::SeqCst) - before;
        assert!(
            layer_fetches <= 1,
            "expected concurrent readers to share one upstream fetch, saw {layer_fetches}"
        );
    }

    /// After the download completes, the blob is a normal cached blob: it is
    /// served from disk and upstream is not contacted again.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn blob_is_cached_after_streaming() {
        let tmp_dir = test_temp_dir!();
        let (addr, blob_requests) = start_mock_upstream(Duration::from_millis(200)).await;

        let trow = start_trow(tmp_dir.as_path_untracked(), addr, true).await.1;
        let repo = format!("f/{addr}/team/app");
        assert_eq!(
            get_manifest_status(&trow, &repo, "1.0.0").await,
            StatusCode::OK
        );

        let fetch = |trow: Router, repo: String| async move {
            let resp = trow
                .oneshot(
                    Request::get(format!("/v2/{repo}/blobs/{}", layer_digest()))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            common::response_body_vec(resp).await
        };

        assert_eq!(fetch(trow.clone(), repo.clone()).await, LAYER_BODY);
        // let the download settle into the blob store
        tokio::time::sleep(Duration::from_millis(400)).await;
        let after_first = blob_requests.load(Ordering::SeqCst);

        assert_eq!(fetch(trow.clone(), repo.clone()).await, LAYER_BODY);
        assert_eq!(
            blob_requests.load(Ordering::SeqCst),
            after_first,
            "second GET must be served from cache without hitting upstream"
        );
    }
}
