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
ENTRYPOINT ["/jams"]
CMD ["--source", "/songs", "--artwork", "/artwork"]
