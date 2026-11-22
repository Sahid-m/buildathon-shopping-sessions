# Build stage: compile the pipeline as a static-ish release binary.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

# Run stage: minimal image, the binary only.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /app/sessions
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/app/sessions"]
