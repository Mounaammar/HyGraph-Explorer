FROM postgres:16

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    flex \
    bison \
    git \
    gnupg \
    libcurl4-openssl-dev \
    libjson-c-dev \
    libreadline-dev \
    libssl-dev \
    postgresql-server-dev-16 \
    wget \
    zlib1g-dev \
    && curl -sSL https://packagecloud.io/install/repositories/timescale/timescaledb/script.deb.sh | bash \
    && apt-get install -y timescaledb-2-postgresql-16 \
    && git clone https://github.com/apache/age.git /tmp/age \
    && cd /tmp/age \
    && make \
    && make install \
    && cd / \
    && rm -rf /tmp/age \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 5432

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["postgres"]