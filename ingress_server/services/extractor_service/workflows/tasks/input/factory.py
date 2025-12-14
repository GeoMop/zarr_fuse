import logging
import tempfile

from pathlib import Path
from typing import List, Optional, Tuple

from airflow.decorators import task

from packages.common.models import MetadataModel

from . import s3_queue
from .models import BaseInputTask, JsonInputTask, CsvInputTask, Hdf5InputTask, InputTaskResult, InputTaskStatus

LOG = logging.getLogger("input_task_factory")

def make_input_task(meta: MetadataModel, source_name: str) -> BaseInputTask:
    ct = (meta.content_type or "").lower()
    if "json" in ct:
        return JsonInputTask(source_name=source_name)
    if "csv" in ct:
        return CsvInputTask(source_name=source_name)
    if "hdf5" in ct or "x-hdf5" in ct:
        return Hdf5InputTask(source_name=source_name)
    raise ValueError(f"Unsupported content_type: {ct}")


def _load_and_validate_metadata(pfx: s3_queue.Prefixes, data_key: str, meta_key: str) -> Optional[MetadataModel]:
    metadata = s3_queue.load_meta(pfx.bucket, meta_key)
    if not metadata:
        LOG.warning("Missing metadata for %s → move to failed.", meta_key)
        s3_queue.move_pair(data_key, meta_key, pfx.bucket, pfx.accepted, pfx.failed)
        return None
    if not metadata.schema_name:
        LOG.warning("Missing schema_name in metadata for %s → move to failed.", meta_key)
        s3_queue.move_pair(data_key, meta_key, pfx.bucket, pfx.accepted, pfx.failed)
        return None
    return metadata


def _download_to_tmp(pfx: s3_queue.Prefixes, endpoint_name: str, data_key: str) -> Tuple[Path, Path]:
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"input_task_{endpoint_name}_"))
    local_path = tmp_dir / Path(data_key).name
    out_dir = tmp_dir / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    s3_queue.download_to_file(pfx.bucket, data_key, local_path)
    return local_path, out_dir


def _process_local_file(endpoint_name: str, metadata: MetadataModel, local_path: Path, out_dir: Path) -> str:
    input_task = make_input_task(metadata, endpoint_name)
    return input_task.process_file(local_path, out_dir)


def _move_to_in_progress(pfx: s3_queue.Prefixes, data_key: str, meta_key: str) -> Tuple[str, str]:
    s3_queue.move_pair(data_key, meta_key, pfx.bucket, pfx.accepted, pfx.in_progress)
    return (
        s3_queue.accepted_to_in_progress_key(pfx, data_key),
        s3_queue.accepted_to_in_progress_key(pfx, meta_key),
    )


@task
def process_input_task(endpoint_name: str, batch_size: int = 10) -> Optional[List[InputTaskResult]]:
    pfx = s3_queue.build_prefixes(endpoint_name)
    items = s3_queue.list_accepted_oldest_first(pfx, batch_size)

    if not items:
        LOG.info("No items to process for endpoint %s", endpoint_name)
        return None

    processed: list[InputTaskResult] = []

    for item in items:
        data_key = item.key
        meta_key = data_key + ".meta.json"

        metadata = _load_and_validate_metadata(pfx, data_key, meta_key)
        if metadata is None:
            LOG.info("Metadata validation failed for metdata: %s", meta_key)
            continue

        try:
            local_path, out_dir = _download_to_tmp(pfx, endpoint_name, data_key)
        except Exception as e:
            LOG.exception("Download failed for %s → move to failed. Error: %s", data_key, e)
            s3_queue.move_pair(data_key, meta_key, pfx.bucket, pfx.accepted, pfx.failed)
            continue

        try:
            data_path = _process_local_file(endpoint_name, metadata, local_path, out_dir)
        except Exception as e:
            LOG.exception("Processing failed %s → move to failed. Error: %s", data_key, e)
            s3_queue.move_pair(data_key, meta_key, pfx.bucket, pfx.accepted, pfx.failed)
            continue

        try:
            inprog_data_key, inprog_meta_key = _move_to_in_progress(pfx, data_key, meta_key)
        except Exception:
            LOG.exception("Move to in_progress failed for %s", data_key)
            continue

        processed.append(
            InputTaskResult(
                bucket=pfx.bucket,
                data_key=inprog_data_key,
                meta_key=inprog_meta_key,
                data_path=data_path,
                metadata=metadata,
            )
        )

    return processed
