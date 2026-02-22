# Build stage
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    git \
    libevent-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY CMakeLists.txt ./
COPY src/ ./src/

RUN mkdir build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    cmake --build . --target pgpooler

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libevent-2.1-7 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/build/pgpooler /usr/local/bin/
RUN mkdir -p /etc/pgpooler /var/log/pgpooler
COPY pgpooler.yaml logging.yaml backends.yaml routing.yaml /etc/pgpooler/

EXPOSE 6432

ENV CONFIG_PATH=/etc/pgpooler/pgpooler.yaml

CMD ["pgpooler"]
