# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# The frontend above is pinned like the images below. Dependabot does not follow
# this line, so it is moved by hand.

# mansio-server as a small non-root image.

# Every image is pinned by digest, with its tag kept to read. Dependabot moves
# both.
#
# The Rust version is written once, in rust-toolchain.toml. The rust image below
# is only a host for rustup: its own compiler is never used. The toolchain file
# is copied in and `rustup toolchain install` installs what it names, so changing
# the pin there changes the image. The check after it fails the build if the
# compiler that will build is not the one the file names. The tag below is not
# the pin and need not move with it.
FROM rust:1.99.1-slim-trixie@sha256:f6c4a35d21dbb3c89d7a8d056c193e94fc09037f2b58dd858b09c39a47dd02d7 AS toolchain
# cargo-chef is installed with `cargo install`, which Dependabot's docker
# ecosystem does not read and cargo-deny (workspace only) does not see. Its
# version is written here and nowhere else, and is moved by hand when a new
# release is wanted (https://crates.io/crates/cargo-chef).
ARG CARGO_CHEF_VERSION=0.1.78
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential cmake pkg-config \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY rust-toolchain.toml ./
RUN rustup toolchain install \
 && channel="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' rust-toolchain.toml)" \
 && { rustc --version | grep -F "rustc ${channel} " \
      || { echo "rust-toolchain.toml pins channel '${channel}' but rustc is '$(rustc --version)'; the channel must be a full X.Y.Z version" >&2; exit 1; }; } \
 && cargo install cargo-chef --version "${CARGO_CHEF_VERSION}" --locked

# The recipe lists the dependencies and nothing else, so it changes only when
# Cargo.toml or Cargo.lock does, and the layer that builds them is kept until then.
FROM toolchain AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM toolchain AS build
COPY --from=planner /src/recipe.json recipe.json
# The dependencies: cached in this layer until the recipe changes. No cache
# mount on target, so that the compiled dependencies stay in the layer, where
# GitHub's Actions cache can carry them to the next CI run. The cost: a local
# rebuild after a source change recompiles the workspace crates (not the
# dependencies, which this layer keeps).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo chef cook --locked --release --package mansio-server --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --locked --release --package mansio-server \
 && install -D target/release/mansio-server /out/mansio-server

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS runtime
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
# the /data volume, with the snapshots of the engine's state. Everything else
# is set by the caller; see the README.
ENV MANSIO_LISTEN=0.0.0.0:7070 \
    MANSIO_SINK_PATH=/data/sink.jsonl \
    MANSIO_SNAPSHOT_DIR=/data/snapshots
EXPOSE 7070
ENTRYPOINT ["mansio-server"]
