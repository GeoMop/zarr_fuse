FROM apache/airflow:slim-3.1.1rc2-python3.11

ENV _PIP_ADDITIONAL_REQUIREMENTS="boto3 pydantic polars python-dotenv"

WORKDIR /opt/airflow
COPY src/common /opt/airflow/src
COPY inputs /opt/airflow/inputs

RUN mkdir -p /opt/airflow/dags
COPY src/extractor_service/dag.py /opt/airflow/dags/ingress_processor.py

ENV PYTHONPATH="/opt/airflow/src:$PYTHONPATH"

ENV INPUTS_DIR="/opt/airflow/inputs"
