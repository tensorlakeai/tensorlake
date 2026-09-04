FROM ubuntu:24.04

ARG RUNNER_VERSION=2.337.0

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        jq \
        libicu74 \
        libssl3 \
        sudo \
        tar \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash runner \
    && echo "runner ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/runner

WORKDIR /opt/actions-runner

RUN curl -fsSL \
        "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" \
        | tar -xz \
    && ./bin/installdependencies.sh \
    && chown -R runner:runner /opt/actions-runner

USER runner

CMD ["sleep", "infinity"]
