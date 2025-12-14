from pathlib import Path
from typing import Any, final
from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict
from packages.common.models import MetadataModel


class BaseInputTask(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    source_name: str

    @final
    def process_file(self, path: Path, out_dir: Path) -> str:
        out_dir.mkdir(parents=True, exist_ok=True)

        raw = self.read_raw(path)
        clean, errors_raw = self.coerce(raw)
        data_path = self.persist(clean, errors_raw, out_dir)

        return data_path

    @abstractmethod
    def read_raw(self, path: Path) -> Any:
        ...

    @abstractmethod
    def coerce(self, raw: Any) -> tuple[Any, Any | None]:
        ...

    @abstractmethod
    def persist(self, clean: Any, errors_raw: Any | None, out_dir: Path) -> str:
        ...
