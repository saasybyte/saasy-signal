# ------------------------------
# Build stage
# ------------------------------
FROM rust:bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y git python3-pip protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Install rustfmt (required by mediasoup-sys build)
RUN rustup component add rustfmt

# Copy dependency manifests
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo

# Copy source code
COPY src ./src

# Build the application
RUN cargo build --release

# ------------------------------
# Runtime stage
# ------------------------------
FROM debian:bookworm-slim

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/saasy-signal ./saasy-signal

# Expose HTTP/WebSocket port
EXPOSE 3000

# Run the application
CMD ["./saasy-signal"]
