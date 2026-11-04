# syntax=docker/dockerfile:1

# Maestra: crates through nora (packages.maestra.io, 3-day hold on new versions,
# issues-maestra#1649). A stage of its own so BuildKit runs it first, while the
# CI-minted OIDC token (BuildKit secret PRIVATE_PACKAGES_TOKEN) is minutes old;
# the build below runs offline and never sees the token. CARGO_SOURCE keys the
# layer cache by source: nora (CI, token required) or crates-io (upstream, local).
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1.94-trixie AS fetch
ARG CARGO_SOURCE=crates-io
WORKDIR /usr/src/trow
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,env=NORA_TOKEN \
    if [ "$CARGO_SOURCE" = nora ]; then \
      [ -n "${NORA_TOKEN:-}" ] || { echo "CARGO_SOURCE=nora needs the PRIVATE_PACKAGES_TOKEN secret" >&2; exit 1; }; \
      printf '%s\n' '[source.crates-io]' 'replace-with = "nora"' \
        '[source.nora]' 'registry = "sparse+https://packages.maestra.io/cargo/index/"' \
        '[registries.nora]' 'index = "sparse+https://packages.maestra.io/cargo/index/"' \
        'credential-provider = ["cargo:token"]' >> "$CARGO_HOME/config.toml"; \
      export CARGO_REGISTRIES_NORA_TOKEN="Basic $(printf 'token:%s' "$NORA_TOKEN" | base64 | tr -d '\n')"; \
    fi &&\
    touch "$CARGO_HOME/config.toml" &&\
    mkdir src && echo "fn main() {}" > src/main.rs &&\
    cargo fetch --locked

# Cross compiling from host platform instead of using qemu
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1.94-trixie AS cross

ARG TARGETARCH
WORKDIR /usr/src/trow

ENV PKG_CONFIG_SYSROOT_DIR=/ \
    SQLX_OFFLINE=true

RUN --mount=type=bind,source=docker/platform.sh,target=/platform.sh \
    /platform.sh

RUN rustup target add $(cat /.platform)

# BASE_REFRESH (CI passes the ISO week): the layer cache would otherwise serve
# this stage forever, without apt/OS security updates. A new value rebuilds it
# on the next build.
ARG BASE_REFRESH
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,required=true \
    --mount=type=bind,from=public.ecr.aws/g5f9s8a4/nora-apt:1.0.0,target=/nora-apt,rw \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean &&\
    . /nora-apt/env.sh && apt-get update &&\
    apt-get install -y unzip openssl $(cat /.compiler)

# --- build ---
COPY Cargo.toml Cargo.lock ./
# Appended, never copied over: platform.sh wrote the cross linkers to the same file.
COPY --from=fetch /usr/local/cargo/config.toml /tmp/cargo-fetch.toml
RUN cat /tmp/cargo-fetch.toml >> /usr/local/cargo/config.toml && rm /tmp/cargo-fetch.toml
COPY --from=fetch /usr/local/cargo/registry /usr/local/cargo/registry
# Trick cargo into building dependencies only
RUN --mount=type=cache,target=./target,id=trow-build-$TARGETARCH \
    mkdir src && echo "fn main() {}" > src/main.rs &&\
    cargo build --release --offline --target $(cat /.platform)
COPY . .
# TODO: Get rid of the "cp" when cargo build --out is stable
RUN --mount=type=cache,target=./target,id=trow-build-$TARGETARCH \
    cargo build --release --offline --target $(cat /.platform) &&\
    cp /usr/src/trow/target/$(cat /.platform)/release/trow /usr/src/trow/



FROM debian:trixie-slim

LABEL org.opencontainers.image.source="https://github.com/trow-registry/trow" \
  org.opencontainers.image.description="Caching Container Registry and Image Management for Kubernetes Clusters" \
  org.opencontainers.image.licenses="Apache-2.0"

RUN groupadd -r -g 2000 trow &&\
    useradd -r -g trow -u 2000 trow

# Note that certs are needed for proxying
ARG BASE_REFRESH
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,required=true \
    --mount=type=bind,from=public.ecr.aws/g5f9s8a4/nora-apt:1.0.0,target=/nora-apt,rw \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    . /nora-apt/env.sh && apt-get update &&\
    apt-get install -y --no-install-recommends openssl ca-certificates

RUN mkdir -p /data /certs &&\
    chown -R trow /data /certs
# keep this late for concurrency
COPY --from=cross /usr/src/trow/trow /trow
USER trow
ENTRYPOINT ["/trow"]
