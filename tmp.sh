#!/bin/bash

run_id="1"
namespace="testing-moc-airflow"
S3_ENDPOINT_URL="https://s3.cl4.du.cesnet.cz"
S3_ACCESS_KEY="4AYEM989E085FY167A5W"
S3_SECRET_KEY="sh7iiSQgJaf8uS9ddQlSRKH9G8GIekR3pxLlDMMv"
BASIC_AUTH_USERS_JSON='{"fiedler-service":"7Zj3?2ho"}'

helm upgrade testing-moc-airflow ingress_server/charts/zarr-fuse-ingress \
  --install --timeout 15m --namespace "${namespace}" \
  --set runId="${run_id}" \
  --set global.s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set global.s3.secrets.accessKey="${S3_ACCESS_KEY}" \
  --set global.s3.secrets.secretKey="${S3_SECRET_KEY}" \
  --set global.fastapi.secrets.usersJson="${BASIC_AUTH_USERS_JSON}" \
  --set ingressService.image.tag="ci-4b3e4da" \
  --set extractorService.airflow.image.tag="ci-4b3e4da" \
  --set extractorService.postgres.secrets.user="admin" \
  --set extractorService.postgres.secrets.password='I^LdfLao6lL0xBQBA9dzlW#vD' \
  --set extractorService.postgres.secrets.database="zarr_fuse_airflow_db" \
  --set extractorService.airflow.secrets.fernetKey="Q4uHNJER436UmFo8l-iYYBtxkRHvxLtYEIcFnbzPJUI=" \
  --set extractorService.airflow.webServer.secrets.webServerSecretKey="np_Pw8yEKc5KX09PqDCTVew5_pi1jgysN-OvosRuyDU" \
  --set extractorService.airflow.initJob.secrets.adminUser.username="admin" \
  --set extractorService.airflow.initJob.secrets.adminUser.password='ZarrAdmin123' \
  --set extractorService.airflow.initJob.secrets.adminUser.email="admin@example.com" \
  --values ingress_server/charts/zarr-fuse-ingress/values/defaults.yaml \
  --values ingress_server/charts/zarr-fuse-ingress/values/testing.yaml
