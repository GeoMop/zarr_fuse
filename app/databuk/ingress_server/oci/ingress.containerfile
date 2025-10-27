FROM python:3.11-slim

ARG APP_VERSION="0.1.0"

WORKDIR /app
COPY packages/common /app/packages/common
COPY services/ingress_service /app/services/ingress_service
COPY inputs /app/inputs

ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir -e /app/packages/common -e /app/services/ingress_service

ENV PYTHONUNBUFFERED=1
ENV AWS_REQUEST_CHECKSUM_CALCULATION=when_required
ENV AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
ENV PYTHONPATH="/app/services/ingress_service:/app/packages/common:$PYTHONPATH"

ENV PORT=8000
EXPOSE ${PORT}

CMD ["python", "-m", "ingress_service.main"]
