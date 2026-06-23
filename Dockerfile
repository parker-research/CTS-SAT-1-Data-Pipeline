FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install general dependencies.
RUN apt-get update
RUN apt-get install -y --no-install-recommends libpq-dev gcc gnuradio software-properties-common python3-apt
RUN rm -rf /var/lib/apt/lists/*

# Install gr-satellites.
RUN add-apt-repository ppa:daniestevez/gr-satellites
RUN apt-get update
RUN apt-get install -y --no-install-recommends gr-satellites
RUN gr_satellites --version

WORKDIR /app

# Store venv outside of the repo so that volume mount doesn't break
# the venv inside and outside the container.
ENV UV_PROJECT_ENVIRONMENT=/venv

ENV PATH="/venv/bin:$PATH"

COPY . /app

RUN uv --version
RUN uv sync --frozen --no-dev

EXPOSE 3000
