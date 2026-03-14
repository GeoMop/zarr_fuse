import yaml
import logging

import polars as pl
from pathlib import Path
from typing import Iterable, Any

from ..app_config import AppConfig
from .context import ExecutionContext, ExecutionContextError
from .active_scrapper_config_models import (
    IterateConfig,
    IterateSchemaConfig,
    IterateDataframeConfig,
    DataSourceConfig,
)

LOG = logging.getLogger("active-scrapper.iterate")


def expand_iterate(
    app_config: AppConfig,
    ctx: ExecutionContext,
    it: IterateConfig,
    data_source: DataSourceConfig,
) -> Iterable[ExecutionContext]:
    if isinstance(it, IterateSchemaConfig):
        return _expand_schema(app_config, ctx, it, data_source)

    if isinstance(it, IterateDataframeConfig):
        return _expand_dataframe(app_config, ctx, it)

    raise ExecutionContextError(f"Unsupported iterate config type: {type(it)}")


def _expand_schema(
    app_config: AppConfig,
    ctx: ExecutionContext,
    it: IterateSchemaConfig,
    data_source: DataSourceConfig,
) -> Iterable[ExecutionContext]:
    LOG.info("Schema path for iterator '%s': %s", it.name, data_source.schema_path)
    schema_file = data_source.resolve_schema_path(app_config.config_dir)
    target_node = it.target_node or data_source.target_node

    values = _schema_extract_values(
        schema_file=schema_file,
        target_node=target_node,
        schema_regex=it.schema_regex,
    )

    return ctx.branch(it.name, values)


def _schema_extract_values(
    schema_file: Path,
    target_node: str | None,
    schema_regex: str,
) -> list[Any]:
    doc = yaml.safe_load(schema_file.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ExecutionContextError(f"Schema file {schema_file} is not a YAML mapping")

    if target_node:
        node = doc.get(target_node)
        if not isinstance(node, dict):
            raise ExecutionContextError(
                f"Schema node '{target_node}' not found in {schema_file}"
            )
    else:
        node = doc

    return extract_by_path(node, schema_regex)


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


def _expand_dataframe(
    app_config: AppConfig,
    ctx: ExecutionContext,
    df_cfg: IterateDataframeConfig,
) -> Iterable[ExecutionContext]:
    if not df_cfg.outputs:
        raise ExecutionContextError("dataframe iterator requires non-empty 'outputs' mapping")

    df_path = df_cfg.resolve_dataframe_path(app_config.config_dir)
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
            raise ExecutionContextError(
                f"Row in dataframe {df_path} is missing columns {missing} required for outputs mapping: {row}"
            )

        yield ctx.with_values(mapping)
