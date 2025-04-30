# zarr-fuse
Prototype project for collecting, dynamic fusion, viewing and publishing of sciantific datasets.

Implemented features:
- allow deffinition af a scientific dataset using a simple YAML based language
- allow merging individual contributions to the dataset, converting to common structure
- example visualization container web app

Future features:
- full support for remote (S3) storage, keys management, CESNET specialities
- generic data collection schemas
  - container accepting instrument data SEND requests, then writes using instument descriptions schema
  - container or local script updating the storage with given CSV, the source should be defined in the dataset schema.
  - container scrap the data, again source should be defined in the dataset schema.
- checking the dataset constraints
- data transforms
- allow selection of subdatasets by various criteria
- expressing data source metadata
- versioning of individual contributions
- ZENODO export /import (with metadata)
- person authentication
- automatized collection of metadata from the format definition and definition of data sources and persons
- generic data visualization building blocks for web apps

- Documentation of raw data collection and merging process.
- Reproducible publication on ZENODO under FAIR principles.

Expected usecases:
- Continuous collection of weather data from yr.no forecast to form a consistent dataset of historical data.
  Allow visualization of different quantities at different locations over different time ranges.
  
- Manual and automatic updates of collected weather data, sensor data and Kalman inversion results.
  Manual updates of the measured data, automatic updates from Kalman inversions.
  
Different techniques and tools will be tried to achieve these goals.

## Install

Clone or download the sources, install from them:
```
pip install .
```

Install from gtihub:

```
pip install git+https://github.com/geomop/zarr_fuse.git
```

To get plotting features or example apps, add these keywords after the "source url" use e.g.
```
pip install .[plotting, apps]
```

`plotting` - provides functions:
- interactive selection of columns in the storage 
- selection from the locations
- multiscale view of time dependent variables


## Development

Run `bash setup_env.sh` in the root of the sources to create development virtual environment.
In particular it installs zarr_fuse as:

```
pip install -e .[dev]
```

The 'dev' dependency includes all dependencies needed for 'plotting' and 'apps' part.


Possible relation to an open source spreadsheet like cloud solutions:
https://chatgpt.com/share/68026c33-3808-8004-a80e-524428ed9de5


## Future improvements
- test parallel writes and reads
- linked storage nodes into xarray tree, e.g. different meteosites and moisture measurments in separate nodes, but allowing 
  to index by common times and locations, but possibly rather support that during read using interpolation if needed.
  

### Direct ZARR - Polars link, without intermediate Pandas DF


ds.to_dask_dataframe().to_parquet() and then pl.scan_parquet()

Lazy Dask Pandas
ds.to_dask_dataframe() → partition-wise map_partitions(pl.from_pandas)
