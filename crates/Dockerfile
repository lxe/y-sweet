FROM rust:slim-bookworm AS builder

WORKDIR /build

COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

# Install CA certificates for SSL/TLS connections
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

COPY --from=builder /build/target/release/y-sweet /usr/local/bin/y-sweet

CMD ["y-sweet", "serve-doc", "--host=0.0.0.0"]
