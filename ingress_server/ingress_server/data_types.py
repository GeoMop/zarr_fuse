from typing import TypeAlias
import polars as pl
import xarray as xr

DataObject: TypeAlias = pl.DataFrame | xr.Dataset
