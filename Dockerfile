# Build a fully static musl binary, then ship it alone in an empty image.
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /usr/src/jams
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN cargo build --release --locked

FROM scratch
COPY --from=builder /usr/src/jams/target/release/jams /jams
USER 65534:65534
EXPOSE 9000
# Where compose.yaml mounts the library and the default artwork
ENV JAMS_SOURCE=/songs JAMS_ARTWORK=/artwork
ENTRYPOINT ["/jams"]
