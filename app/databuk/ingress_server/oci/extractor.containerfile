FROM apache/airflow:2.10.2-python3.11

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
WORKDIR /opt/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends git=1:2.39.5-0+deb12u2 && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root packages/common packages/common
COPY --chown=airflow:root inputs inputs
COPY --chown=airflow:root services/extractor_service services/extractor_service

USER airflow

RUN pip install --no-cache-dir -e packages/common -e services/extractor_service && \
    pip install --no-cache-dir apache-airflow-providers-amazon==9.16.0

RUN ln -s services/extractor_service/dags /opt/airflow/dags
