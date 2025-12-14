from pathlib import Path
import polars as pl
from .tabular_input_task import TabularInputTask

class CsvInputTask(TabularInputTask):
    def _read_polars(self, path: Path) -> pl.DataFrame:
        return pl.read_csv(path)
