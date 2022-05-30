FROM rust:latest

WORKDIR /opt/filebroker-server
COPY /. ./

RUN cargo build --release

CMD ["cargo", "run", "--release"]
