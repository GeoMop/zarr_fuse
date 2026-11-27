# zarr-fuse features and use cases

## Why build a shared dataset?

A collection of source files records what each instrument, team, or model contributed. Comparing observations across
those files means interpreting source-specific conventions every time. zarr-fuse lets a project define those conventions
once and apply them to successive contributions. Team members can identify quantities, select observations, and see how
coverage grows as data arrive.

## Ingress and data curation

1. **Collect.** The Flask-based ingress service accepts submissions over HTTP and can make scheduled HTTP requests to
   collect data. It performs initial checks, such as rejecting empty payloads and malformed JSON, and queues inputs for
   processing. The queue can use a local directory or S3-compatible object storage.
2. **Extract.** The worker reads formats such as CSV and JSON into Polars tables. Projects can supply Python extractors
   for source-specific parsing and cleaning. The worker also supports xarray datasets, such as those read from GRIB
   weather files.
3. **Filter.** A source can identify a column or coordinate that represents the time of the data. The worker uses it to
   order queued contributions. An optional retention window holds recent batches until sufficiently newer data arrive,
   giving late files time to reach the queue. This is based on the time in the data.
4. **Check and merge.** For tabular updates, the YAML schema maps source columns to dataset variables, declares their
   dimensions and data types, and can specify units, allowed ranges, and missing-value conventions. The library uses
   Pint for physical-unit conversion. Missing required coordinates can cause an update to fail. If converting a table
   column fails, the library can log the problem and represent the column as missing. The worker records processing
   failures in the queue for inspection. Extractors that produce xarray datasets can prepare values and apply
   source-specific checks before passing them to the merge step.

[Schema syntax](zf_schema.md) · [Ingress configuration](../ingress_server/README.md) ·
[Ingress deployment](ingress-server-deployment.md)

## How xarray and Zarr fit together

An **xarray dataset** holds related variables that can share dimensions. A temperature variable might use `time`,
`borehole`, and `depth`, while a site description uses only `borehole`. Coordinates provide the labels along those axes.
Shared labels let researchers align arrays for comparison or select data by date, borehole name, or depth. Attributes
carry information such as a variable's units or description.

The zarr-fuse library uses the schema and coordinates to turn table rows into arrays and merge contributions into the
target group. An update can add coordinate values, such as later observation times, or supply measurements for existing
coordinates. The schema can group related datasets within one store.

**Zarr** stores the arrays and their metadata on a local filesystem or in S3-compatible object storage. Each array is
split into chunks. A reader can open a group as an xarray dataset and access a subset by reading the chunks it touches.
**Dask** can defer loading and computation until the selected data are needed.

[Storage configuration](store_vars.md) · [Example multidimensional schema](../zarr_fuse/test/inputs/schema_tensors.yaml)

## Dashboard and scientific use cases

The browser dashboard uses **Panel**, **HoloViews**, and **GeoViews** from the HoloViz ecosystem. A YAML view describes
which store and dataset group to open. It maps dataset fields to roles such as time, site, depth, and geographic
location. For datasets with these coordinates, the dashboard can link selection on a map to time-series plots.

- **Borehole monitoring.** The repository's DataBuk example collects online measurements from the Bukov underground
  research laboratory. Its dashboard configuration lets researchers inspect rock temperatures by borehole and depth
  and compare series over a common time period. [DataBuk example](../dashboard/test_deployment/databuk/README.md)
- **Weather records.** Collect repeated downloads into a historical dataset. A project can keep forecast issue time and
  the time the forecast applies to as separate dimensions when it needs to compare forecasts for the same hour.
- **Simulation ensembles.** Organize outputs by time, location, and model run. With the run represented as a dimension,
  downstream analysis can summarize results across runs or select runs for model training and evaluation.

[Dashboard setup](../dashboard/README.md)

## Project direction

The project vision discusses provenance and publication under FAIR principles. The versioning designs explore using
Icechunk to track and reproduce store snapshots.

[Rationale and long-term vision](vision.md) · [Versioning design](versioning/ZFWorkspace_design.md)
