# syntax=docker/dockerfile:1

FROM rust:trixie

ARG FFMPEG_VERSION=filebroker-9.0.1-2

#
# Install filebroker FFmpeg.
#
# install.sh deliberately refuses to run as root and uses sudo itself,
# so create a temporary normal user with passwordless sudo.
#
RUN apt-get update && \
    apt-get install -y \
        sudo \
        ca-certificates \
        curl && \
    useradd --create-home --shell /bin/bash ffmpeg-installer && \
    echo "ffmpeg-installer ALL=(ALL) NOPASSWD:ALL" \
        > /etc/sudoers.d/ffmpeg-installer

USER ffmpeg-installer

RUN curl -fsSL \
        "https://github.com/filebroker/FFmpeg/releases/download/${FFMPEG_VERSION}/install.sh" \
        -o /tmp/install-ffmpeg.sh && \
    chmod +x /tmp/install-ffmpeg.sh && \
    /tmp/install-ffmpeg.sh

USER root

###################################
# filebroker-server dependencies. #
###################################
RUN apt-get update && \
    apt-get install -y \
        libpq-dev \
        libimage-exiftool-perl

############################
# Build filebroker-server. #
############################
WORKDIR /opt/filebroker-server

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/opt/filebroker-server/target \
    cargo build \
        --locked \
        --release \
        --features auto_migration && \
    cp target/release/filebroker-server /usr/local/bin/filebroker-server

CMD ["/usr/local/bin/filebroker-server"]
