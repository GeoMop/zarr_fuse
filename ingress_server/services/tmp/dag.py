import logging
import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.decorators import task, task_group
from dotenv import load_dotenv

from packages.common import configuration, logging_setup
from workflows.io import ingress_processing, queue_s3

LOG = logging.getLogger("dag_extractor")

load_dotenv()

with DAG(
    dag_id="ingress_processor",
    description="Process accepted objects and store to Zarr via zarr_fuse (3-step pipeline)",
    start_date=datetime(2025, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 2},
    tags=["ingress", "s3"],
) as dag:
    logging_setup.setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    @task()
    def select_objects(endpoint_name: str, batch_size: int = 10) -> dict[str, Any]:
        pfx = queue_s3.build_prefixes(endpoint_name)

        items = queue_s3.list_accepted_oldest_first(pfx.bucket, pfx.accepted, batch_size)
        if not items:
            LOG.info("Endpoint %s: nic ke zpracování", endpoint_name)
            return {
                "bucket": pfx.bucket,
                "accepted_prefix": pfx.accepted,
                "to_process": [],
                "to_fail": [],
            }

        selected = items[:batch_size]

        to_process: list[dict[str, Any]] = []
        to_fail: list[dict[str, str]] = []

        for item in selected:
            data_key = item.key
            meta_key = data_key + ".meta.json"

            meta = queue_s3.load_meta(pfx.bucket, meta_key)
            if not meta:
                LOG.warning("Missing meta for %s → will move to failed.", data_key)
                to_fail.append({"data_key": data_key, "meta_key": meta_key})
                continue

            if not meta.schema_name:
                LOG.warning("Missing schema_name in meta for %s → will move to failed.", data_key)
                to_fail.append({"data_key": data_key, "meta_key": meta_key})
                continue

            to_process.append(
                {
                    "data_key": data_key,
                    "meta_key": meta_key,
                    "schema_name": meta.schema_name,
                    "node_path": (meta.node_path or "").strip(),
                    "content_type": (meta.content_type or "application/json").lower(),
                }
            )

        LOG.info(
            "Endpoint %s: vybráno %d objektů (%d k procesování, %d k failu)",
            endpoint_name,
            len(selected),
            len(to_process),
            len(to_fail),
        )

        return {
            "bucket": pfx.bucket,
            "accepted_prefix": pfx.accepted,
            "to_process": to_process,
            "to_fail": to_fail,
        }

    @task()
    def process_to_zarr(selection: dict[str, Any]) -> dict[str, Any]:
        from packages.common.models import MetadataModel  # lokálně, aby nebyly cykly

        bucket = selection["bucket"]
        to_process: list[dict[str, Any]] = selection["to_process"]

        processed_keys: list[str] = []

        for item in to_process:
            data_key = item["data_key"]
            meta_key = item["meta_key"]

            # načti znovu meta – ale klidně by se daly posílat jako dict v selection
            meta = queue_s3.load_meta(bucket, meta_key)
            if not meta:
                LOG.warning("Meta disappeared for %s → přesuneme jako failed v dalším kroku", data_key)
                continue

            try:
                payload = queue_s3.read_bytes(bucket, data_key)
                # meta už je MetadataModel, ale kdyby ne:
                if not isinstance(meta, MetadataModel):
                    meta = MetadataModel.model_validate(meta)
                ingress_processing.process_payload(meta, payload)
                processed_keys.append(data_key)
            except Exception as e:
                LOG.exception("Error processing %s: %s", data_key, e)
                # necháme task failnout – retry na úrovni DAGu
                raise

        return {
            "bucket": bucket,
            "accepted_prefix": selection["accepted_prefix"],
            "processed_keys": processed_keys,
            "to_fail": selection["to_fail"],
        }

    @task()
    def move_objects(endpoint_name: str, selection: dict[str, Any], result: dict[str, Any]) -> str:
        pfx = queue_s3.build_prefixes(endpoint_name)

        bucket = result["bucket"]
        accepted_prefix = result["accepted_prefix"]
        processed_keys: list[str] = result["processed_keys"]
        to_fail: list[dict[str, str]] = selection["to_fail"]

        for item in to_fail:
            data_key = item["data_key"]
            meta_key = item["meta_key"]
            queue_s3.move_pair(bucket, data_key, meta_key, pfx.failed, accepted_prefix)
            LOG.info("Moved %s to failed", data_key)

        for data_key in processed_keys:
            meta_key = data_key + ".meta.json"
            queue_s3.move_pair(bucket, data_key, meta_key, pfx.processed, accepted_prefix)
            LOG.info("Moved %s to processed", data_key)

        msg = (
            f"{endpoint_name}: processed {len(processed_keys)} object(s), "
            f"failed {len(to_fail)} object(s)."
        )
        LOG.info(msg)
        return msg

    @task_group
    def build_pipeline(source_name: str):
        sel = select_objects.override(task_id=f"select_objects__{source_name}")(
            endpoint_name=source_name,
            batch_size=10,
        )
        res = process_to_zarr.override(task_id=f"process_to_zarr__{source_name}")(
            selection=sel,
        )
        move_objects.override(task_id=f"move_objects__{source_name}")(
            endpoint_name=source_name,
            selection=sel,
            result=res,
        )


    for cfg in configuration.load_united_data_source_config():
        build_pipeline(cfg.name)
