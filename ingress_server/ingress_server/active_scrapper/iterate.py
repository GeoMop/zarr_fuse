import yaml
import logging

import polars as pl
from pathlib import Path
from typing import Iterable, Any

from .context import ExecutionContext, ExecutionContextError
from .active_scrapper_config_models import (
    IterateConfig,
    IterateSchemaConfig,
    IterateDataframeConfig,
    DataSourceConfig,
)

LOG = logging.getLogger("active-scrapper")

def expand_iterate(
    ctx: ExecutionContext,
    it: IterateConfig,
    data_source: DataSourceConfig,
) -> Iterable[ExecutionContext]:
    if isinstance(it, IterateSchemaConfig):
        return _expand_schema(ctx, it, data_source)

    if isinstance(it, IterateDataframeConfig):
        return _expand_dataframe(ctx, it)

    raise ExecutionContextError(f"Unsupported iterate config type: {type(it)}")


def _expand_schema(
    ctx: ExecutionContext,
    it: IterateSchemaConfig,
    data_source: DataSourceConfig,
) -> Iterable[ExecutionContext]:
    LOG.info("Schema path for iterator '%s': %s", it.name, data_source.schema_path)
    schema_file = data_source.get_schema_path()
    schema_node = it.schema_node or data_source.schema_node

    values = _schema_extract_values(
        schema_file=schema_file,
        schema_node=schema_node,
        schema_regex=it.schema_regex,
    )

    return ctx.branch(it.name, values)


def _schema_extract_values(
    schema_file: Path,
    schema_node: str,
    schema_regex: str,
) -> list[Any]:
    doc = yaml.safe_load(schema_file.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ExecutionContextError(f"Schema file {schema_file} is not a YAML mapping")

    node = doc.get(schema_node)
    if not isinstance(node, dict):
        raise ExecutionContextError(
            f"Schema node '{schema_node}' not found in {schema_file}"
        )

    return extract_by_path(node, schema_regex)


def _expand_dataframe(
    ctx: ExecutionContext,
    df_cfg: IterateDataframeConfig,
) -> Iterable[ExecutionContext]:
    if not df_cfg.outputs:
        raise ExecutionContextError("dataframe iterator requires non-empty 'outputs' mapping")

    df_path = df_cfg.get_dataframe_path()
    try:
        df = pl.read_csv(df_path, has_header=df_cfg.dataframe_has_header)
    except Exception as e:
        raise ExecutionContextError(f"Failed to read dataframe {df_path}: {e}") from e

    for row in df.iter_rows(named=True):
        mapping = {}
        missing = []
        for ctx_key, col_name in df_cfg.outputs.items():
            val = row.get(col_name)
            if val is None:
                missing.append(col_name)
            mapping[ctx_key] = val

        if missing:
            LOG.info(
                "Skipping dataframe row due to missing columns %s in %s: %s",
                missing, df_path, row
            )
            continue

        yield ctx.with_values(mapping)


def extract_by_path(obj: Any, path: str) -> list[Any]:
    parts = [p for p in path.split(".") if p]
    current: list[Any] = [obj]

    for part in parts:
        next_level: list[Any] = []

        if part == "*":
            for item in current:
                if isinstance(item, dict):
                    next_level.extend(item.values())
                elif isinstance(item, list):
                    next_level.extend(item)
            current = next_level
            continue

        for item in current:
            if isinstance(item, dict) and part in item:
                next_level.append(item[part])
        current = next_level

    return current
