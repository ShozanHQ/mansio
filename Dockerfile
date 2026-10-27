# syntax=docker/dockerfile:1

# mansio-server as a small non-root image.

# Keep in step with rust-toolchain.toml. The toolchain file is not copied in,
# so the image's own compiler is the one that builds.
ARG RUST_VERSION=1.99.0

FROM rust:${RUST_VERSION}-slim-trixie AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential cmake pkg-config \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --package mansio-server \
 && install -D target/release/mansio-server /out/mansio-server

FROM debian:trixie-slim AS runtime
# curl is for the container health check.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin mansio \
 && mkdir /data \
 && chown mansio:mansio /data
COPY --from=build /out/mansio-server /usr/local/bin/mansio-server
USER mansio
WORKDIR /data
# Listen on every interface inside the container, and keep the sink file on
# the /data volume. Everything else is set by the caller; see the README.
ENV MANSIO_LISTEN=0.0.0.0:7070 \
    MANSIO_SINK_PATH=/data/sink.jsonl
EXPOSE 7070
ENTRYPOINT ["mansio-server"]
