# FROM rust:trixie AS builder
# WORKDIR /usr/src/jams
# COPY . .
# RUN apt-get -y update
# RUN apt-get -y upgrade
# RUN apt-get install -y ffmpeg libavutil-dev libavformat-dev libavfilter-dev libavdevice-dev clang
# RUN cargo install --path .

FROM rust:slim-trixie AS builder
WORKDIR /usr/src/jams
COPY . .
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt install -y pkg-config ffmpeg libavutil-dev libavformat-dev libavfilter-dev libavdevice-dev clang
RUN cargo install --path .

FROM rust:slim-trixie
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt install -y ffmpeg
COPY --from=builder /usr/local/cargo/bin/jams /usr/local/bin/jams
CMD ["jams"]
