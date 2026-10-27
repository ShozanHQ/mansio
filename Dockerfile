# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# The frontend above is pinned like the images below. Dependabot does not follow
# this line, so it is moved by hand.

# mansio-server as a small non-root image.

# Every image is pinned by digest, with its tag kept to read. Dependabot moves
# both together. The Rust version is kept in step with rust-toolchain.toml by
# hand: the toolchain file is not copied in, so the image's own compiler is the
# one that builds.
FROM rust:1.99.0-slim-trixie@sha256:2752b332db73fdbb7dc576f06c82ed1f312005784ef913d7e04a28f5f55dc581 AS build
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

FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS runtime
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
