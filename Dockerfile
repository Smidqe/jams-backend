# Build a fully static musl binary, then ship it alone in an empty image.
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /usr/src/jams

# Build dependencies against stub sources first, so this layer stays cached
# until Cargo.toml or Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src src
# Make sure cargo rebuilds the real sources rather than reusing the stub
RUN touch src/main.rs && cargo build --release --locked

FROM scratch
COPY --from=builder /usr/src/jams/target/release/jams /jams
USER 65534:65534
EXPOSE 9000
ENTRYPOINT ["/jams"]
CMD ["--source", "/songs", "--artwork", "/artwork"]
