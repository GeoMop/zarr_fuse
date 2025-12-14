import h5py
import xarray as xr

from pathlib import Path
from typing import Any

from .base_input_task import BaseInputTask


class Hdf5InputTask(BaseInputTask):
    def read_raw(self, path: Path) -> h5py.File:
        return h5py.File(path, "r")

    def coerce(self, raw: h5py.File) -> tuple[h5py.File, None]:
        return raw, None

    def persist(self, clean: h5py.File, errors_raw: Any | None, out_dir: Path) -> str:
        zarr_path = out_dir / "data.zarr"

        try:
            dset = clean["/some/group/data"]
            chunks = dset.chunks or (min(dset.shape[0], 1024),) + dset.shape[1:]
            da = xr.DataArray(
                dset,
                dims=("time", "x", "y"),
                name="my_var",
                attrs=dict(clean["/some/group"].attrs),
            ).chunk(dict(zip(da.dims, chunks))) if hasattr(xr.DataArray, "chunk") else xr.DataArray(dset, dims=("time","x","y"), name="my_var")

            ds = da.to_dataset()

            ds.to_zarr(zarr_path, mode="w")

            return str(zarr_path)

        finally:
            clean.close()
