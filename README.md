# zarr-fuse

## Overview

![Raw data → Curated by ingress → Merged data in Zarr → Dashboard](docs/images/data-processing.png)

**zarr-fuse** is a Python library and web application for the collaborative collection and publication of scientific
datasets according to the [FAIR principles](https://www.gofair.foundation/fair-principles).

**Development status.** Version 1.0 is targeted for June 2027. The features below describe the planned release.
Use current releases for evaluation only.

1. **Define the dataset.** A data steward creates the data schemas and configures the data curation pipelines using
   built-in filters or custom Python code.
2. **Contribute data.** Contributors can upload files through a web interface generated from the input data schema.
   Continuous data streams can be set up for instruments and services.
3. **Curation and merge.** `Ingress` automatically standardizes incoming data and checks it against the input schema.
   The configured pipeline can then merge each contribution into the common dataset while preserving links to the
   original dataset.
4. **Inspect and revise.** Use the ZF-view dashboard to review merged or intermediate datasets. Fix the data or adjust
   the pipeline, then rerun the pipeline.

### Define the dataset

A schema records what each quantity means, how it is indexed, which units to use, and which values are allowed. The
data steward uses it to define a common target for sources that report the same quantity in different formats or units.
Each source can have its own input schema and processing settings, with custom Python steps for instrument-specific
formats.

### Contribute data

A monitoring station can send readings as they arrive, the ingress service can fetch a weather feed on a schedule, and a
field team can submit a CSV after a campaign. These routes let teams combine long-running monitoring with occasional
field measurements in the same dataset.

### Curation and merge

The pipeline can convert depths reported in centimetres to metres and check that each measurement has a borehole
identifier. A time filter can order late arrivals by observation time, and processing logs help the team trace failed
checks back to a contribution. See [Ingress](ingress_server/README.md).

| Data model | Example dataset |
| --- | --- |
| **xarray** labels the axes. **Zarr** stores the arrays in chunks. | ![Multidimensional dataset][example-schema] |

Temperature and pore pressure can share named dimensions such as time, borehole, and depth. The coordinates are the
labels along those axes, such as actual timestamps or depths in metres. Each variable uses the dimensions it needs.
zarr-fuse uses the coordinate labels to place new observations, and Zarr's chunks let readers load a subset such as
one week of data from one borehole.

### Inspect and revise

In the Bukov monitoring example, researchers can select boreholes on a map and compare temperature series at chosen
depths. Gaps or unexpected patterns give the team a place to start investigating. See [ZF-view](dashboard/README.md).

The planned Icechunk snapshot history will let the team compare dataset states before and after a correction.
Persistent data and view identifiers will let the team refer to the exact dataset version and dashboard view used in
an analysis. For publication, teams can deposit that dataset version with its provenance and descriptive metadata in a
research data repository.

[example-schema]: docs/images/example_schema.png

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
