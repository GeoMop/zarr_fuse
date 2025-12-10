FROM docker.io/library/alpine:3.22.1

ARG APP_VERSION="devel"

LABEL org.opencontainers.image.title="ZarrFuse Dashboard - Frontend"
LABEL org.opencontainers.image.description="Frontend for the ZarrFuse Dashboard."
LABEL org.opencontainers.image.version="${APP_VERSION}"
LABEL org.opencontainers.image.licenses="BSD-3-Clause"
LABEL org.opencontainers.image.source="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.url="https://github.com/geomop/zarr_fuse"
LABEL org.opencontainers.image.authors="Geomop / Stepan Moc <stepan.mocik@gmail.com>"
LABEL maintainer="Geomop / Stepan Moc <stepan.mocik@gmail.com>"

RUN addgroup -S nginx && adduser -S -D -H -h /nginx -u 11233 -G nginx nginx

RUN apk add --no-cache nginx curl && \
    mkdir -p /var/www/html

COPY --chown=nginx:nginx dist/ /var/www/html
COPY --chown=nginx:nginx oci/nginx.conf /etc/nginx

RUN ln -sf /dev/stdout /var/log/nginx/access.log \
  && ln -sf /dev/stderr /var/log/nginx/error.log

EXPOSE 8091

USER nginx

CMD [ "nginx", "-g", "daemon off;" ]
