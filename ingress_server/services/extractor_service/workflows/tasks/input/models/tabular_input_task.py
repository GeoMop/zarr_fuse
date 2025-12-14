from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import ConfigDict

from .base_input_task import BaseInputTask


def _coerce_types_with_errors(
    df: pl.DataFrame,
    column_types: dict[str, pl.DataType] | None = None,
    nav_values: dict[str, list[Any]] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    if column_types is None:
        return df, None

    nav_values = nav_values or {}
    corrected = df.clone()
    error_rows: list[dict[str, Any]] = []

    for col_name, dtype in column_types.items():
        if col_name not in df.columns:
            continue

        col = df[col_name]
        nav_vals = nav_values.get(col_name, [])

        try:
            casted = col.replace(nav_vals, None).cast(dtype, strict=False)
            corrected = corrected.with_columns(casted.alias(col_name))
        except Exception:
            for idx, val in enumerate(col):
                if val in nav_vals:
                    continue
                error_rows.append(
                    {"_row": idx, "column": col_name, "value": val, "error": "type_coercion_failed"}
                )

    error_df = pl.DataFrame(error_rows) if error_rows else None
    return corrected, error_df


class TabularInputTask(BaseInputTask, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    column_types: dict[str, pl.DataType] | None = None
    nav_values: dict[str, list[Any]] = {}

    def read_raw(self, path: Path) -> pl.DataFrame:
        return self._read_polars(path)

    def coerce(self, raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame | None]:
        return _coerce_types_with_errors(raw, self.column_types, self.nav_values)

    def persist(
        self,
        clean: pl.DataFrame,
        errors_raw: pl.DataFrame | None,
        out_dir: Path,
    ) -> str:
        data_path = out_dir / "data.parquet"
        clean.write_parquet(data_path)

        if errors_raw is not None:
            errors_path = out_dir / "errors.parquet"
            errors_raw.write_parquet(errors_path)

        return str(data_path)

    @abstractmethod
    def _read_polars(self, path: Path) -> pl.DataFrame:
        ...
