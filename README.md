# zarr-fuse
Prototype project for collecting, dynamic fusion, viewing and publishing of sciantific datasets.

Implemented features:
- allow definition of a scientific dataset using a simple YAML based language
- allow merging individual contributions to the dataset,
  converting to common structure
- example visualization container web app

Near term future:
- test more complex updates, non-continuous arrays
  Coords are not automaticaly sorted !!
  Updating tables could have different time stepping in each update!!
  Merge with sort preserved
  Allow to intepolate table to common coordinates with given time step.
- Refactor schema of single ds to a dedicated class
- method to get schema of whole dataset
- review of the schema YAML syntax
- full support for remote (S3) storage, keys management, CESNET specialities
- unify design of plotting classes

Changes in SCHEMA:
- move complete COORD spec into COORDS, Undrestand coords as extendet VARS


Contemplated features:
- move to TileDB backend with native support for checkpointing/versioning and sparse dimensions
  [ChatGPT research](https://chatgpt.com/share/68173f9f-3748-8004-ab29-4cedd87ce136)
- test support for vast datasets and parallel IO

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
- Metadata schemata: DublinCore, Data Cite, DCAT, DDI (TODO: review)

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

Run `bash tools/setup_env.sh` in the root of the sources to create development virtual environment.
In particular it installs zarr_fuse as:

```
pip install -e .[dev]
```

The 'dev' dependency includes all dependencies needed for 'plotting' and 'apps' part.


Possible relation to an open source spreadsheet like cloud solutions:
https://chatgpt.com/share/68026c33-3808-8004-a80e-524428ed9de5

---

### Running S3 Viewer Locally with Docker

For local development and testing with S3-compatible storage, you can use [rclone](https://hub.docker.com/r/rclone/rclone) in a Docker container to expose an S3 browser UI.
Below are instructions to set up and run the rclone S3 web GUI using your custom rclone.conf.

#### Prerequisites
- Docker installed and running.
- An S3-compatible bucket and credentials.
- rclone.conf file configured for your S3 provider (see example below).


#### Example rclone.conf

Create a file at ~/rclone.conf with the following content (update credentials as needed):
```ini
[s3]
type = s3
provider = Other
access_key_id = access_key
secret_access_key = secret_key
endpoint = https://s3.cl4.du.cesnet.cz
region = du
```

#### Running the S3 Viewer

Run the following command in your terminal:

```sh
docker run -d -p 5572:5572 -v ~/rclone.conf:/config/rclone/rclone.conf rclone/rclone:latest rcd --rc-web-gui --rc-addr :5572 --rc-user admin --rc-pass heslo123
```

#### Notes

This will start the rclone remote control (rcd) with the web GUI on http://localhost:5572.

Login using:
```ini
Username: admin
Password: heslo123
```

The S3 remote defined in your rclone.conf (here, [moje_s3]) will be available for browsing and management.






## CESNET S3

### Members
[Perun](https://perun.e-infra.cz) -> "Access management" on the left -> organisation VO_tul_scifuse -> Members

- invite members
- or users can apply for membership through [the form](https://perun.cesnet.cz/fed/registrar/?vo=VO_tul_scifuse)
- membership is valid in 30 minutes after [approval](https://docs.du.cesnet.cz/en/docs/perun/user-approval) by the group administrator [

Key management policy:
- each user generates its own keys
- each user only see his own keys
- CESNET could attribute keys to users 

## Future improvements
- test parallel writes and reads
- linked storage nodes into xarray tree, e.g. different meteosites and moisture measurments in separate nodes, but allowing
  to index by common times and locations, but possibly rather support that during read using interpolation if needed.


### Direct ZARR - Polars link, without intermediate Pandas DF


ds.to_dask_dataframe().to_parquet() and then pl.scan_parquet()

Lazy Dask Pandas
ds.to_dask_dataframe() → partition-wise map_partitions(pl.from_pandas)
