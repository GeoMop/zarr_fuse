FROM python:3.11-slim

ARG APP_VERSION="0.1.0"

LABEL org.opencontainers.image.title="Ingress Server"
LABEL org.opencontainers.image.description="FastAPI service for CSV/JSON ingestion into an S3-backed Zarr store (no UI)."
LABEL org.opencontainers.image.version="${APP_VERSION}"
LABEL org.opencontainers.image.licenses="BSD-3-Clause"
LABEL org.opencontainers.image.source="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.url="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.authors="Geomop / Stepan Moc <stepan.mocik@gmail.com>"
LABEL maintainer="Geomop / Stepan Moc <stepan.mocik@gmail.com>"

WORKDIR /app

COPY packages/common packages/common
COPY services/ingress_service services/ingress_service
COPY inputs inputs

ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir -e packages/common -e services/ingress_service

ENV PYTHONUNBUFFERED=1
ENV AWS_REQUEST_CHECKSUM_CALCULATION=when_required
ENV AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
ENV PYTHONPATH=/app

RUN adduser --disabled-password --gecos "" --uid 1000 ingress
USER ingress

WORKDIR /app/services
ENV PORT=8000
EXPOSE ${PORT}

CMD ["python", "-m", "ingress_service.main"]
