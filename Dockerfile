# The orbita image: one binary, no entrypoint script, no init system.
#
# Two stages. The builder needs more than a Rust toolchain: protoc, because the
# gRPC bindings are generated at build time from the definitions in /proto, and
# a C++ toolchain with clang, because RocksDB is compiled from source and
# bindgen needs libclang to read its headers. The runtime image needs none of
# that, which is most of the reason for splitting them.
#
# Build for both architectures with buildx:
#
#   docker buildx build --platform linux/amd64,linux/arm64 -t orbita:dev .

FROM rust:1.91-bookworm AS builder

# clang and libclang are for bindgen, g++ and make come from build-essential
# for the RocksDB sources, and protobuf-compiler is protoc.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        clang \
        libclang-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Fetch dependencies in their own layer. The manifests change far less often
# than the source, so an edit to a .rs file does not re-download the world.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/orbita-core/Cargo.toml crates/orbita-core/
COPY crates/orbita-runtime/Cargo.toml crates/orbita-runtime/
COPY crates/orbita-objectstore/Cargo.toml crates/orbita-objectstore/
COPY crates/orbita-proto/Cargo.toml crates/orbita-proto/
COPY crates/orbita-storage/Cargo.toml crates/orbita-storage/
COPY crates/orbita-wal/Cargo.toml crates/orbita-wal/
COPY crates/orbita-control/Cargo.toml crates/orbita-control/
COPY crates/orbita-server/Cargo.toml crates/orbita-server/
COPY crates/orbita-sim/Cargo.toml crates/orbita-sim/
COPY crates/orbita-cli/Cargo.toml crates/orbita-cli/
RUN mkdir -p crates/orbita-cli/src \
    && echo 'fn main() {}' > crates/orbita-cli/src/main.rs \
    && echo '' > crates/orbita-cli/src/lib.rs \
    && for c in core runtime objectstore proto storage wal control server sim; do \
         mkdir -p "crates/orbita-$c/src" && echo '' > "crates/orbita-$c/src/lib.rs"; \
       done \
    && cargo fetch --locked

COPY proto proto
COPY crates crates

# Only the binary is built. The simulator and the test suites are CI's job, and
# building them here would put their dependencies in the image's build cache
# for nothing.
RUN cargo build --release --locked --bin orbita \
    && strip target/release/orbita

FROM debian:bookworm-slim AS runtime

# ca-certificates is for talking to an S3-compatible object store over TLS.
# Nothing else is installed, because everything installed is something to
# patch.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# A fixed uid and gid, so that a Kubernetes securityContext and a bind-mounted
# host directory can both name the same number and mean the same thing.
RUN groupadd --gid 10001 orbita \
    && useradd --uid 10001 --gid 10001 --home-dir /var/lib/orbita --shell /usr/sbin/nologin orbita \
    && mkdir -p /var/lib/orbita \
    && chown 10001:10001 /var/lib/orbita

COPY --from=builder /src/target/release/orbita /usr/local/bin/orbita

USER 10001:10001
WORKDIR /var/lib/orbita

# The data directory is a volume so that a container restart is not a data loss
# event even without an orchestrator.
VOLUME ["/var/lib/orbita"]

# Client, admin, and peer traffic share one port. A second port is a second
# thing to expose and firewall, and gRPC multiplexes services on one connection
# anyway.
EXPOSE 7100

ENV ORBITA_DATA_DIR=/var/lib/orbita \
    ORBITA_LISTEN=0.0.0.0:7100

# No shell in the entrypoint. The binary is pid 1 and receives signals
# directly, which is what makes a graceful shutdown possible.
ENTRYPOINT ["/usr/local/bin/orbita"]
CMD ["serve"]
