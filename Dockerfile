# syntax=docker/dockerfile:1
# Single `ant` binary: server (`ant serve`) and client.
FROM rust:1.95-bookworm AS builder
WORKDIR /src
COPY . .
# Distribution build: no debuginfo, stripped.
ENV CARGO_PROFILE_RELEASE_DEBUG=false \
    CARGO_PROFILE_RELEASE_STRIP=symbols
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p ant-cli \
 && cp target/release/ant /ant

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 ca-certificates git \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /ant /usr/local/bin/ant
# gRPC and HTTP object endpoint; bind publicly inside the container.
ENV ANT_GRPC_LISTEN=0.0.0.0:7420 \
    ANT_HTTP_LISTEN=0.0.0.0:7421
EXPOSE 7420 7421
ENTRYPOINT ["ant"]
CMD ["--help"]
