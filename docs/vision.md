# Zarr-Fuse motivation and concepts

## Rationale
Goal is to collect various physical quantities data for stochastic modelling
and training neural network models.


### Problems to solve

**data from measurements**
- need simple data updates through (Excel/CSV) tables
- deal with human factor, lack of consistency

**data from similations**
- updates are large datasets
- updates comes in parallel from many cluster machines

**stochastic analysis**
- data overview, visualization, and inspection for exploratory analysis
- computing estimates over collected populations (reduction along sample axis)

**machine learning**
- enable to derive various dataset subsets for training/validation of the models

**FAIR data**
- collect all necessary metadata about data origin
- enable streamlined publication on repositories
- data lineage (tracking data processing steps form the source

## Data processing workflow

1. **Ingress** 
    - raw data contribution
    - dataframe = coordinates and quantities in named columns, values in rows
      e.g. pandas, polars, parquet
    - sparse data format
    - measurement: CSV, excel table
    - simulation: slice of the full array
    - specific services to realize efficient and reliable ingress
    - put raw data into input journal

2. **Validation and harmonization**
    - apply automatic data transformstions to fit into specifications of the dataset
      e.g. interpolate to given discrete coordinates
    - units conversion
    - check expectations about data, hold invalidate data in the input journal 
    - put transformed data into output journal 
    - machine produced datasets could skip transformation and validation, or undergo it only randomly

3. **Merge**
    - sparse validated dataframes merged to the versioned storage, composing dense datasets at least in some coordinates
    
4. **Inspection**
    - basic plotting components and tools for data overview
    - priarly web application plotting
    - desiged with reusability in mind 

    
    
## Data as virtual N-dimensional array
For their specificity, we conceptualy distinguish five types of cordinates (aaxes of the array).
Ve assume that each indivudual number of the dataset has unique combination of these five coordinates.
Moreover these coordinates types are clearly independent

**Time** 
- essentialy 1D coordinate, sortable
- usualy primary appendable coordinate
- multiscale postprocessing (derived subcoordinates like day of year, hour of day, ...)
- multiscale view/plot
- timezones, time formats
- time instance, vs. time interval
- real time, simulated time

**Location**
- non-sortable
- allowing discrete locations, physical coordinates like labels
- location in space
- multiscale like time, can have more then 3D, e.g. XYZ of borehole head + position of sensor in hte borehole
- use of "composed" coordinate, ( ?? possible problems of  unstable hashing due to rounding errors)
- could be modeld either as xarray coord or as ZARR folder

**Realization**
- stochastic realization
- imagine like parallel universe realization
- duplicit measurements in "nearly" same location

**Source**
- measurement vs. simulations
- sensor ID (+ metadata)
- simulation ID (+ metadata)
- similarities to discrete location
- usually common to whole update dataframe

**Quantity** 
- named
- unit
- possible comparison of same quantity in dfferent locations in common plot

## Technical solution

- use Kubernetes containers for ingress services, scalable for various ingress types and intensity
- separate containers for input transformation and validation (postponed)
  candidate tools: Pandera, GX, Pachyderm, Dagster
- use xarrays + zarr + icechunk for the storage:
  - merge of contribution dataframes
  - read slices
  - automatic down sampling
  - house keeping
- postprocessing (postponed, Pachyderm, Dagster)
  - downsampling
  - statistics
  - offline analysis
- 


## Relevant projects

### Storage
**Xarray + Zarr** 

GOOD: current solution, scalability, parallel IO, Python API

MISSING: versioning, sparse updates

**IceChunk** (Apache) - layer on top of ZARR providing consistent versioning and timetravel, support tags (mutable refs to shapshots) and branches (immutable refs)
Seems to be fit our needs.
Scalability tips: 
- Chunk size 1-16MB (amortize per chunk versioning overhead). 
- Branch per writer; needs merge later on, but not supported yet by IceChunk, ZarrFuse could do that as part of Garbage collection - fuse smaller parralllel work branches
  by rebasing to main. Better to solve using 'merge_sessions'
- Enable manifest splitting along most common dimensin (time?)
- Regular garbage collector, expiration
- See [introduction blog](https://earthmover.io/blog/icechunk)

**LakeFS** Paied service with open source library. Whole service not just data layer. But support many storage formats not only ZARR.
To generic in direction we do not need.

**TileDB** Well documeted supprot for both dense arryas like ZARR and sparse arrays (like Parquet), support for versioning and more.
Not sure about suport of very large datasets due to advertising to many features. Differs from Xarray API, but there is an [interface lib](https://github.com/TileDB-Inc/TileDB-CF-Py)

**Apache Iceberg** Storage based on Parquet files

**Delta Lake**

**ZARR-v3** There are proposals towards native versioning [issue #154]https://github.com/zarr-developers/zarr-specs/issues/154)

**Arrow Flight** transfer of large datasets over networks, could be usefull for largescae updates from simulations.

### Ingress worflow

[**Pachyderm**](https://github.com/pachyderm/pachyderm) Provide a virtual filesystem for versioning and storing raw data and trigger transformation on updates.
Can be applied with GX or Pandera to transform and validate input dataframes or for deriving statistics and subsampled data from the main IceChunk storage.

**Dagster** General workflow dataprocessing, like `Apache Airflow`. Better local testing, debugging, containerization, more complex dependency.

**OpenLineage + Marquez** DataLineage - tracking data transforms done by Airflow, spark or dbt.

**Great Expectations** Provides expectations for various table like formats as YAML config file, not great composition mechanisms, but usable.

**Pandera** Python lib for transforming (parsing) and validation of dataframes (pandas, pyspark, polars). Primary write schema in Python, but serializable to YAML.
 That seems to be more flexible but also more coding than  GX.
 
[**Frictionless**](https://frictionlessdata.io/) Small scale date, limited interoperability. Inferior to previous tools and thir combinations.


## [Plotting](./plotting.md)
### Brianstorming notes

Starting stack:
Jupyter
Voila
HoloViz Panel’s GridStack / FastGridTemplate (figures as dynamic tiles)
Bokeh 

HLAVO:
Map:
- selection of nodes to plot in the map:
    - weather station
    - soil profile measurements
    - wells
    - simulation grid
    - moisture measured interpolated
    - moisture simulated

Surface plot:    
- selection of up to three points to plot from map
- single point: time comparison of measure and predicted quantity
- selection of quantities to plot
- selection of depths to plot (in single figure)
- time series correlation analysis (for two quantities)

Well plot:
- up to three well timelines
- time series correlation analysis (for two quantities)
- compare measurement and prediction
