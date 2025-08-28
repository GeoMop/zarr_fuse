# Syntax of Zarr Fuse Schema YAML file

## Tree
The ZARR storage is a tree of nodes, each node holds a multidimensional dataset.
ZARR storage schema follows this tree structure, it is composed of a tree of dictionaries corresponding 
to nodes/groups of the Zarr storage.

## Node schema
A Node Schema directory consists of keys for its child nodes and optional reserved keys: [ATTRS, COORDS, VARS] 
defining structure of the node's dataset.


- **ATTRS** Dictionary of custom attributes not processed by zarr-fuse directly. Zarr-fuse could add its own attributes 

- **VARS** Variables of the Dataset

- **COORDS** Coordinates of the Dataset

## ATTRS
User can store any custom node attributes, but some attributes are interpretted by the zarr_fuse package:

- All evironment variables (see bellow) could be provided in ATTRS, the environment variable overwrites the ATTRS value.
- Schema specific options, empty right now.


### Environment variables
- `STORE_URL` is a store url passed to `zarr.storage.FsspecStore`. Supported are:
    - **S3 store** url in form `s3://<bucket_name>/<store_path>`, e.g. `s3://test_bucket/project_XYZ/case_storage.zarr`
    - **Zip store** (not supported yet), url with `zip://` prefix, followed by realtive or absolute path on the local filesystem
    - **local store** url without any `<prefix>://` prefix, could be a relative or absolute path to the zarr store root folder on the local filesystem, e.g. `./
    
- `S3_ACCESS_KEY` and `S3_SECRET_KEY` is standard AWS secretes pair. These could be part of attributes, but that is highly insecure and would produce a warning.

- `S3_ENDPOINT_URL` the https url of the S3 gateway
- `S3_OPTIONS` - optional JSON string encoding dictionary of detailed storage options passed as kwargs to `zarr.storage.FsspecStore`:
    
                'listings_expiry_time': 1,
            # Timeout of the folder listing cache in seconds.
            # Affects zarr.open_group if the unconsolidated data are in use.
            'max_paths': 0,
            # Number of cached folders. We effectively disable caching by setting it to 0.
            'asynchronous': True,
            # !? Should be rather False
            'config_kwargs': {
                # 's3': {
                #     'payload_signing_enabled': False,
                #     # Default False. Some endpoints may require signing set to True.
                #     'addressing_style': os.getenv('S3_ADDRESSING_STYLE'),
                #     # Values: 'auto', 'path', 'virtual'; 'auto' is default.
                # },
                'retries': {'max_attempts': 5, 'mode': 'standard'},
                # max_attemps defult is 3, mode 'standard' is default.
                # use 'adaptive' for networks with time varying latency.
                #'connect_timeout': 20,
                # Timeout for establishing a connection in seconds. Default 60.
                #'read_timeout': 60,
                # Timeout for reading data in seconds. Default 60.
                'request_checksum_calculation': 'when_required',
                'response_checksum_validation': 'when_required',
                # Checksum validation modes:
                # 'when_required' - only if the server needs checksums; needed for non-AWS endpoints
                # 'when_supported' - prefers checksums if the server supports them
            }


## Example
```
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
