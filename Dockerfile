# Statically compile with optimisations in the build image
FROM clux/muslrust:1.72.0 AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --release

# Copy static binary from build image into minimal Debian-based image
FROM alpine:latest
COPY --from=builder \
    /build/target/x86_64-unknown-linux-musl/release/ocypod-server \
    /usr/local/bin/
EXPOSE 8023
ENTRYPOINT ["/usr/local/bin/ocypod-server"]
