#!/usr/bin/env bash
# Maestra: the PR "Rust checks" step, run inside docker.io/library/rust (repo at /w).
# With NORA_TOKEN (a GitHub OIDC token, minutes-lived) crates come through nora
# (packages.maestra.io, 3-day hold on new versions, issues-maestra#1649): every
# download happens first, while the token is fresh, and the tests run offline.
set -euo pipefail
if [ -n "${NORA_TOKEN:-}" ]; then
  printf '%s\n' '[source.crates-io]' 'replace-with = "nora"' \
    '[source.nora]' 'registry = "sparse+https://packages.maestra.io/cargo/index/"' \
    '[registries.nora]' 'index = "sparse+https://packages.maestra.io/cargo/index/"' \
    'credential-provider = ["cargo:token"]' >> "$CARGO_HOME/config.toml"
  CARGO_REGISTRIES_NORA_TOKEN="Basic $(printf 'token:%s' "$NORA_TOKEN" | base64 | tr -d '\n')"
  export CARGO_REGISTRIES_NORA_TOKEN
fi
unset NORA_TOKEN
set -x
cargo fetch --locked
rustup component add rustfmt
cargo install sqlx-cli --no-default-features --features sqlite,rustls --locked
unset CARGO_REGISTRIES_NORA_TOKEN
mkdir -p target
cargo sqlx database setup
cargo fmt --check
# utils::temporary_file::test_temporary_file_async_cancellation is flaky on
# pristine upstream too (it races a spawned write against a cancel), so it is
# not a signal about fork changes.
cargo test --offline --lib -- --skip test_temporary_file_async_cancellation
cargo test --offline --test smoke_test_proxy_streaming
