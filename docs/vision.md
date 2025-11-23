# Zarr-Fuse motivation and concepts

## Rationale
Goal is to collect various physical quantities data for stochastic modelling
and training neural network models.


### Problems to solve

**data from measurements**
- need simple data updates through (Excel/CSV) tables
- deal with human factor, lack of consistency

**data from simulations**
- updates are large dense datasets
- updates comes in parallel from many cluster machines

**stochastic analysis**
- data overview, visualization, and inspection for exploratory analysis
- computing estimates over collected populations (reduction along sample axis)

**machine learning**
- enable to derive various dataset subsets for training/validation of the models

**FAIR data**
- collect all necessary metadata about data origin
- enable streamlined publication on repositories
- data lineage (tracking data processing steps form the source)
- level of metadata organization should be scalable allowing nearly no metadata up to very complex
  metadata schemas
  
  
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
For their specificity, we conceptualy distinguish five types of cordinates (axes of the array).
we assume that each indivudual scalar value within a dataset has unique combination of these five coordinates.
Moreover these coordinates types are clearly independent

**Time** 
- essentialy single 1D coordinate, sortable
- often primary appendable coordinate
- multiscale postprocessing (derived subcoordinates like day of year, hour of day, ...)
- multiscale view/plot
- timezones, time formats
- time instance, vs. time interval
- real time, simulated time

**Location**
- non-sortable
- allowing discrete locations, physical coordinates like labels
- location in space
- multiscale nature similar to the Time, can have more then 3D, e.g. XYZ of borehole head + position of sensor in hte borehole
- use of "composed" coordinate, ( ?? possible problems of  unstable hashing due to rounding errors)
  See also [new indices developments](https://xarray-indexes.readthedocs.io/#xarray-diagram-wild) in XArray. Possibly
  these will serve better to our purpose.
- could be modeled either as xarray coord or as ZARR folder

**Realization**
- stochastic realization
- imagine like parallel universe realization
- duplicit measurements in the "nearly" same location

**Provenience**
- measurement vs. simulations
- sensor ID (+ metadata)
- simulation ID (+ metadata)
- similarities to discrete location
- usually common to whole update dataframe

**Quantity** 
- named
- unit
- possible comparison of same quantity in dfferent locations in common plot

## Metadata association
There are various metadata schemas and we have no support for particular metadata schema
but the data store schema file should allow to model what kind of metadata are collected.
The metadata could be in priciple viewed as other variables, but with specific means of store as
they are common for large parts of the arrays, but on other hand there could be lot of them and 
are not of primary importance. 

**Metadata granularity**
The metadata variables or their collections could be attributed to data of various extension.
The granularity of a metadata variable specify extension of the data unit that could hold
a value different from other units of the same extension. 
Following are possible granularity options with examples of metadata variables that could have such granularity.
However granularity of the metadata is always on the designer of the dataset schema.

- **store**     One value for whole zarr-fuse storage. E.g. access rights, project 
- **dataset**   One value for a dataset within the zarr-fuse storage tree. 
                Datasets / groups could separate different fidelity of description or individual parts of some system. 
                Metadata examples: resolution level, simulation configuration 
- **variable**  One value for each variable in each dataset. 
                E.g. unit, dtype, range, definition of the quantity, function relating it to other variables (in the same or different datasets), workflow history,
                controled / dependent, precision, resolution
- **update**    One value for each update of each variable of each dataset.
                E.g. measurement device, computing HW, identity of operator, simulation batch
- **slice / single value** 
                Probably not metadata anyway, but regular variable. Could be a variable that is assumed to not affect other variable, but recorded for reference or
                an auxiliary varaible. E.g. temperature readings of pressure sensors -- of lower imprtance in undergraoud with very small temperature variation,
                nevertheless used in the pressure evaluation formula.
                
**Data Provenience Kinds**
Let's discus little more possible metadata associated with **Provenience** axis. 
which is often sparse and have metadata character. We can distinguish several cathegories of 
the provenience metadata.
- **design**    Metadata that are part of the data collection design.
- **responsibility** Who contributes these data? Person, organisation.
- **essential** Metadata variables that could essentialy affect the collected values. E.g. pH or temperature of concentrations measurements. 
                Consider to treat these as coordinates or regular variables. 
- **technical** Could affect colected values, but effect is neglected. E.g. measuring device, resolution, simulation tolerances
- **circumstances** Variables assumend to not affecting the data. E.g. temperature of pressure sensing (see above)
                

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
