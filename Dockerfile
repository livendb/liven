# ── Build stage ──
FROM node:24-bookworm AS ui-builder

WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --legacy-peer-deps
COPY ui .
RUN npm run build

FROM rust:latest AS builder

WORKDIR /app

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY tests ./tests

# Copy pre-built UI assets
COPY --from=ui-builder /ui/dist ./ui/dist

# Build release binary
RUN cargo build --release --locked && \
    strip target/release/liven

# ── Runtime stage ──
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Create LIVEN user and directories
RUN groupadd -r liven && \
    useradd -r -g liven -d /var/lib/liven -s /sbin/nologin liven && \
    mkdir -p /var/lib/liven /var/log/liven /etc/liven && \
    chown -R liven:liven /var/lib/liven /var/log/liven && \
    chmod 700 /var/lib/liven /var/log/liven

# Copy binary and config
COPY --from=builder /app/target/release/liven /usr/local/bin/liven
COPY liven.toml /etc/liven/liven.toml

# Expose ports (db_port, webui_port)
EXPOSE 43121 43120

# Default config path
ENV LIVEN_CONFIG=/etc/liven/liven.toml

# Switch to non-root user
USER liven

# Default command
ENTRYPOINT ["/usr/local/bin/liven"]
CMD ["start", "--config", "/etc/liven/liven.toml"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD /usr/local/bin/liven status || exit 1
