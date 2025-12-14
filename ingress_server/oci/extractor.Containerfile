FROM docker.io/apache/airflow:3.1.1-python3.11

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

USER 50000:root
WORKDIR /opt/app

COPY --chown=50000:root packages/common ./packages/common
COPY --chown=50000:root inputs ./inputs
COPY --chown=50000:root services/extractor_service ./services/extractor_service

ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir -e ./packages/common -e ./services/extractor_service && \
    pip install --no-cache-dir "apache-airflow-providers-amazon==9.18.1" "apache-airflow-providers-fab==3.1.0"

# && \
#     pip install --no-cache-dir 'apache-airflow==3.1.1' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.1/constraints-3.11.txt"

COPY --chown=50000:root ./services/extractor_service/dags /opt/airflow/dags

ENV SCHEMAS_DIR=/opt/app/inputs/schemas
ENV PYTHONUNBUFFERED=1
ENV AWS_REQUEST_CHECKSUM_CALCULATION=when_required
ENV AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
ENV PYTHONPATH=/opt/app
