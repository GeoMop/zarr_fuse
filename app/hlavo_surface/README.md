# Colletiong weather and field soil moisture mesurement

Use ZARR fuse to compare scrapped yr.no forecast data to historical meteosat and nasa.


## Install environemnt
```
bash setup_env 
```

## Install environment (on Windows)
Here is how to install the environment in portable way, 
manually using only python. 
Assuming you are in the root dir of 'hlavo_surface' app:

```
python3 -m venv "venv"
venv/bin/python3 -m pip install -e ../..[plot] 
venv/bin/python3 -m pip install -e .
```

This will:

2. Create virtual environment in folder "venv"
2. install zarr_fuse in editable mode
3. install hlavo_surface in editable mode

## Scrapper scripts

Placed in `scrappers`.

`python3 -m hlavo_surface.scrap.weather`
Scrapes yr.no forecast data for a given location and time range.
Currently saves to local store under 'workdir' folder.



**TODO**

`python3 -m hlavo_surface.scrap.soil DIR`
Collect CSV files with moisture measurements from DIR.

## Visualization Notebook Prototype

To run the visualization prototype, start the jupyter with:
```
bash jupyter.sh
```
Assumes the the local python environment is created (see 'Install environment').
Assumes 'jupyter-lab' be installed and accessible through the system PATH.

## Voila application
Try resulting Voila! application:
```
venv/bin/voila notebooks/zoom_plot.ipynb
```

## TODO list (of hlavo_surface)
- S3 store, CESNET
- continuous yr.no scrap (GitHub Action on:schedule)
- meteosat data
- soil data
- compare yr.no and meteosat
- read for Richards: interpolate from close locations, read from single meteo and soil node
  linear interpolation in time
- zarr_fuse support for read from multiple nodes into single table, single
  base node or explicit coordinates values, interpolation


- add more detection of schema errors (e.g. empty of inconsistent ds schema
- optional logger parameter to deserialize
- node logger instances with node path part of messages
- test parallel writes and reads



- logging: lag all warning and errors into the dataset or common Zarr log array.
  log cols: date_time, level, messge
  log info for each update operation, optionaly keep all update tables unmerged as well
  or one can have have some more refined policy for merging tree of datasets (yes this could be use case for xarray dataset tree)

Zarr-fuse stabilization:
- test repreated updates, value overwriting, unique coordinates
- problem with consolidation
- logging
- review whole code

Notebook:


4. overview for more datasets (separate nodes)
5. selection for more datasets (tree view + boxcheck)
6. zoom in map

2. prompt for node path in th sourece 
3. load DF from the node, use 



Shortcommings:
- use scrapper function according to node ATTR

- zarr_storage creation create uncomplete under temp name,
  rename after completion
- Implement special keys by general resolution mechanism,
  after YAML parsing. Use that mechanism to implement ATTRS, COORDS, VARD, FN, REF
  Choose better names, possibly custom tag.
  Have derived 'dict' implementation, possibly taken from bgem.
  Idea is to have possibility to call Python or Visip functions,
  assume pure functions, no state.
  Then we can have realtively general update machanism configured through the file.
  So the schema also describes the update step.
- ZARR complains:
  /home/jb/workspace/zarr_fuse/zarr_fuse/zarr_storage.py:367: RuntimeWarning: Failed to open Zarr store with consolidated metadata, but successfully read with non-consolidated metadata. This is typically much slower for opening a dataset. To silence this warning, consider:
1. Consolidating metadata in this existing store with zarr.consolidate_metadata().
2. Explicitly setting consolidated=False, to avoid trying to read consolidate metadata, or
3. Explicitly setting consolidated=True, to raise an error in this case instead of falling back to try reading non-consolidated metadata.
  return xr.open_zarr(self.store, group=rel_path)
/home/jb/workspace/zarr_fuse/zarr_fuse/zarr_storage.py:367: RuntimeWarning: Failed to open Zarr store with consolidated metadata, but successfully read with non-consolidated metadata. This is typically much slower for opening a dataset. To silence this warning, consider:
1. Consolidating metadata in this existing store with zarr.consolidate_metadata().
2. Explicitly setting consolidated=False, to avoid trying to read consolidate metadata, or
3. Explicitly setting consolidated=True, to raise an error in this case instead of falling back to try reading non-consolidated metadata.
  return xr.open_zarr(self.store, group=rel_path)
