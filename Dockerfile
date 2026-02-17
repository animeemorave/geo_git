FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    git \
    wget \
    ca-certificates \
    libcurl4-openssl-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN wget https://github.com/mongodb/mongo-c-driver/releases/download/1.24.4/mongo-c-driver-1.24.4.tar.gz && \
    tar xzf mongo-c-driver-1.24.4.tar.gz && \
    cd mongo-c-driver-1.24.4 && \
    mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local .. && \
    make -j$(nproc) && \
    make install && \
    cd / && rm -rf /tmp/mongo-c-driver-1.24.4*

RUN wget https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.8.1/mongo-cxx-driver-r3.8.1.tar.gz && \
    tar xzf mongo-cxx-driver-r3.8.1.tar.gz && \
    cd mongo-cxx-driver-r3.8.1 && \
    mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DCMAKE_CXX_STANDARD=17 .. && \
    make -j$(nproc) && \
    make install && \
    cd / && rm -rf /tmp/mongo-cxx-driver-r3.8.1*

WORKDIR /app

COPY CMakeLists.txt ./
COPY src ./src

RUN mkdir -p build && cd build && \
    cmake .. && \
    make

CMD ["/app/build/geoversion", "mongodb://mongodb:27017"]
