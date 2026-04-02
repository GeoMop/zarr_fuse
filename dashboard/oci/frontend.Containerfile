FROM docker.io/library/python:3.11-slim

ARG APP_VERSION="devel"

LABEL org.opencontainers.image.title="ZarrFuse HoloViz - Frontend"
LABEL org.opencontainers.image.description="Panel frontend for the HoloViz dashboard."
LABEL org.opencontainers.image.version="${APP_VERSION}"
LABEL org.opencontainers.image.licenses="BSD-3-Clause"
LABEL org.opencontainers.image.source="https://github.com/geomop/zarr_fuse"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY . /app

WORKDIR /app/dashboard
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5006
CMD ["panel", "serve", "app.py", "--address", "0.0.0.0", "--port", "5006", "--allow-websocket-origin=*" ]
