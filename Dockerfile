FROM rust:1.92-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release -p kevindb-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/kevindb-server /usr/local/bin/kevindb-server

ENV KEVINDB_BIND_ADDR=0.0.0.0:3000
EXPOSE 3000

ENTRYPOINT ["kevindb-server"]
