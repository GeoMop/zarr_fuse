# Airflow base už obsahuje airflow + constraints
FROM apache/airflow:2.9.3-python3.11

USER root
WORKDIR /opt/app

# kód
COPY packages/common /opt/app/packages/common
COPY services/extractor_service /opt/app/services/extractor_service
COPY inputs /opt/app/inputs

# doplňkové knihovny (mimo airflow core)
ENV _PIP_ADDITIONAL_REQUIREMENTS="boto3 pydantic polars pyyaml python-dotenv"
# nainstaluj kód jako editable balíčky
RUN pip install --no-cache-dir -e /opt/app/packages/common -e /opt/app/services/extractor_service

# DAGy dej tam, kde je Airflow očekává
RUN mkdir -p /opt/airflow/dags
RUN ln -s /opt/app/services/extractor_service/extractor_service/dag.py /opt/airflow/dags/ingress_processor.py

# ENV
ENV SCHEMAS_DIR=/opt/app/inputs/schemas
ENV PYTHONPATH="/opt/app/packages/common:/opt/app/services/extractor_service:$PYTHONPATH"

USER airflow
