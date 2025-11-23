FROM docker.io/apache/airflow:2.10.2-python3.11

ARG APP_VERSION="0.1.0"

LABEL org.opencontainers.image.title="Extractor Service"
LABEL org.opencontainers.image.description="Airflow DAG for processing ingested data from S3-backed Zarr store."
LABEL org.opencontainers.image.version="${APP_VERSION}"
LABEL org.opencontainers.image.licenses="BSD-3-Clause"
LABEL org.opencontainers.image.source="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.url="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.authors="Geomop / Stepan Moc <stepan.mocik@gmail.com>"
LABEL maintainer="Geomop / Stepan Moc <stepan.mocik@gmail.com>"

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git=1:2.39.5-0+deb12u2 && \
    rm -rf /var/lib/apt/lists/*

USER 1000:1000

COPY --chown=1000:root packages/common /opt/app/packages/common
COPY --chown=1000:root inputs /opt/app/inputs
COPY --chown=1000:root services/extractor_service /opt/app/services/extractor_service

ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir -e /opt/app/packages/common -e /opt/app/services/extractor_service

COPY --chown=1000:root services/extractor_service/dags /opt/airflow/dags

ENV SCHEMAS_DIR=/opt/app/inputs/schemas
ENV PYTHONUNBUFFERED=1
ENV AWS_REQUEST_CHECKSUM_CALCULATION=when_required
ENV AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
ENV PYTHONPATH=/opt/app
