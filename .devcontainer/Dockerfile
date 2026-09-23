FROM ghcr.io/philips-software/amp-devcontainer-rust:8.3.0

# Chromium system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libasound2t64 \
    libgbm1 \
    libcairo2 \
    libpango-1.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Download + verify + extract
ARG CHROMIUM_VERSION=150.0.7871.186-1
ARG CHROMIUM_SHA256=54ffb53f7c9b7d0148cef6c221977045a0dc4e805de55ed065350b22b87e8570

ADD https://github.com/ungoogled-software/ungoogled-chromium-portablelinux/releases/download/${CHROMIUM_VERSION}/ungoogled-chromium-${CHROMIUM_VERSION}-x86_64_linux.tar.xz /tmp/chromium.tar.xz

RUN echo "${CHROMIUM_SHA256} /tmp/chromium.tar.xz" | sha256sum -c - && \
    mkdir -p /opt/chromium && \
    tar -xf /tmp/chromium.tar.xz -C /opt/chromium --strip-components=1 && \
    ln -s /opt/chromium/chrome /usr/local/bin/chromium && \
    rm /tmp/chromium.tar.xz

# Smoke test
RUN chromium --headless --disable-gpu --no-sandbox --dump-dom https://example.com > /dev/null