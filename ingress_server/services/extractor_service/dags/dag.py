import os
import logging

from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from packages.common import logging_setup
#from extractor_service.workflows.tasks.input import factory

LOG = logging.getLogger("dag_extractor")

load_dotenv()

with DAG(
    dag_id="ingress_listener",
    description="Listens for new accepted objects in S3 and triggers processing DAG",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["listener", "s3"],
) as dag:
    logging_setup.setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    wait_for_s3_files = S3KeySensor(
        task_id="wait_for_s3_files",
        bucket_key="s3://app-databuk-test-service/queue/accepted/**",
        aws_conn_id=os.getenv("AIRFLOW_AWS_CONN_ID", ""),
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 60 * 24 * 365,
        mode="reschedule",
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_extracting_process",
        trigger_dag_id="trigger_extracting_process",
        conf={
            "note": "Triggered by S3PrefixSensor",
        },
    )

    wait_for_s3_files >> trigger_processing

with DAG(
    dag_id="trigger_extracting_process",
    description="Triggered DAG to extract accepted objects from S3 and store to Zarr via zarr_fuse",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=3,
    tags=["ingress", "s3"],
) as processing_dag:
    logging_setup.setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    @task
    def log_start():
        """
        Only for testing purposes.
        Log the start of the processing DAG.
        """
        LOG.error("Processing DAG started by trigger.")
