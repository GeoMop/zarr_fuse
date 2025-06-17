# Syntex of Zarr Fuse Scehma YAML file

## Tree
Schema is composed of a tree of dictionaries corresponding to Nodes/groups of the Zarr storage.

## Node schema
Dictionaries with reserved keys: [ATTRS, COORDS, VARS] define a dataset Node. These defines structure of 
an xarray Dataset of the Node.  


**ATTRS** Dictionary of custom attributes not processed by zarr-fuse directly.
**VARS** Variables of the Dataset
**COORDS** Coordinates of the Dataset

## Example
```aiignore
# Configuration of the ZARR-FUSE storage
ATTRS:
    # storage URL, not processed automaticaly by the library to connect to storage
    store_url: "surface.zarr"
    store_type: 'local'                 # 'guess' type is default
    
yr.no:
    VARS:
        # We put forward varaible keys as valid keywords, but quoted names are valid in YAML
        precipitation_amount:
            unit: "mm"
            description: "parcipitation in [mm] per hour (!VERIFY!)"
            coords: ["date_time", "lat_lon"]
            
        longitude:
            unit: "deg"
            description: "Signed Earth longitude."
            coords: "lat_lon"
            
        latitude:
            unit: "deg"
            description: "Signed Earth longitude."
            coords: "lat_lon"
            # df_col = <var's name is default source data frame column>
            # coords = <var's name is self coordinated variable> 
        
        date_time:
            unit: "TimeStamp"
            description: "Time coordinate"
        
    
    COORDS:
        date_time: # Defines coord from the variable of the same name. 
            chunk_size: 1024
            # Single components are sorted by default
            sort: True
            # Replace values in overalp interval between current and new values.
            # Assumes sorted coordinate.  
            merge: "replace_interval"
            # Union of current and new values.
            # merge: "union" 
            # merge: "interpolate_to_current"
            # merge: "interpolate_to_new"
            
        lat_lon:
            # Secondary description valid for composed coordinates. 
            description: "Example of coordinate indexed uniquely by a tuple of values. The source columns are converted to the 1D list of tuples."
            # Composed coordinates allows to form "sparse" coordinates points with coordinates 
            # given by the variables in the 'composed' list. 
            composed: ["latitude", "longitude"]
            # Sparse coordinate should be unsorted. 
            # However we sort hash coordinates anyway to detect overlap.
            #sort: False 
            # Disable introducing new values.
            merge: False
            chunk_size: 512
        
    ATTRS:
        description:
            "Most recent 6h forcast data from yr.no collected into continuou historical data series."
        update:
            FN: yr_no_update
            module: weather_scrapper
            url: https://api.met.no/weatherapi/locationforecast/2.0/complete
            html_cache: False    
            locations:
              FN: location_df
              #module: "."   # default relative to calling function module
              attrs_dict: *loc_file     #!Ref .ATTRS    # Helm style variable interpolation. 
#meteosat:



```