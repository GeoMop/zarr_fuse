FROM docker.io/library/python:3.11-slim-bullseye

ARG APP_VERSION="devel"

LABEL org.opencontainers.image.title="ZarrFuse Dashboard - Backend"
LABEL org.opencontainers.image.description="Backend for the ZarrFuse Dashboard."
LABEL org.opencontainers.image.version="${APP_VERSION}"
LABEL org.opencontainers.image.licenses="BSD-3-Clause"
LABEL org.opencontainers.image.source="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.url="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.authors="Geomop / Stepan Moc <stepan.mocik@gmail.com>"
LABEL maintainer="Geomop / Stepan Moc <stepan.mocik@gmail.com>"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

USER root
WORKDIR /app

COPY backend/ /app/

RUN apt-get update && \
    apt-get install -y --no-install-recommends git=1:2.30.2-1+deb11u2 && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir /app

RUN chown -R 1000:1000 /app

USER 1000
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
