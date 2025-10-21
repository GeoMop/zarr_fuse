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
User can store any custom attributes per node, these are then accessible as the xarray dataset attributes.
The attribute endcoding and decoding mechanism support not only values of scaler types (str, float),
but also dictionaries and lists.

### Storage Instance Varaibles
The root node ATTRS could contain [special variables](./store_vars.md) (all in uppercase) configuring the storage instance, 
namely S3 connection details. 

## VARS
The VARS dictionary defines variables of the dataset stored in the node.
Each variable is defined by its own dictionary of properties.

### `coords`
String or list of strings, name(s) of the coordinate(s) indexing the variable. 
The variable is N dimensional tensor, where each of N axes corresponds to one named coordinate defined in COORDS.

### `type` (optional)
Explicit data type of the variable in terms of Numpy dtype. 
Default is 'None', meaning the dtype is determined by the first write. 
We accept also following types for non-physical quantities:
  [`bool`, `int`, `int8`, `int32`, `int64`, `float`, `float64`, `complex`, `str[n]` ]. 
  Here `str[n]` defines fixed length UTF8 string of length n, 
  longer strings are truncated on write to the store. The variable length strings have support in the ZARR and
  might be supported in future versions.
If  `unit` is provided, the `bool` and `str[n]` are not allowed.
Explicit type specification is recommended.

### `invalid_value`
Value used to represent missing or invalid data in the variable. Native NaN is used by default for float quantities.
'signed_max_int' and 'signed_min_int' keywords are supported for integer types, representing the maximum and minimum 
signed integer values of the variable type. Otherwise you have to provide the actual value compatible with the variable type.

### `unit` 
Could be string or dictionary. The second case is used to define more complex variable values. 

- **string** physical unit of the variable, [pint package](https://pint.readthedocs.io/en/stable/) 
  syntax is applied.

- **Date Time Unit** 
  Defines a variable with time stamp values, internally using [Numpy datetime64](https://numpy.org/doc/stable/reference/arrays.datetime.html) type. 
  The unit is recognized by the obligatory key 'tick'. DateTime values could be converted from floats or from strings using [date util parser](https://dateutil.readthedocs.io/en/stable/parser.html)
  Following keys are recognized:

  - `tick` : String with the time resolution, it defines the tick in `datetime64[<tick>]`;
    The tick affects resolution after conversion from string POSIX values.
  - `tz` : String defining time zone of the stored times, default is UTC and that is recommended for the storage values. 
    However, one can also define units for the input data, and this allows to specify e.g. different data sources with different 
    timezons converted to the consistent dataset.   
  - `dayfirst` : bool, default = False; Interpret the first value as day (set to True), or as the month (set to False).
  - `yearfirst` : bool, default = True; If true YY is at the frist place, else it is at the last place. 

### `range` (optional)
  - `discrete`: If set, the variable could only take values from provided set. 
     The set could either be defined as a list of values, or as a string containing 
     path to a CSV file which would be used to populate the set of discrete values.
     Also the feature  primarily enable categorical string values, the discrete sets of ints, 
     floats or complex numbers are supported as well.
     The keys `df_col` and `source_unit` are used for extracting the and converting the values.
     Discrete values are stored as int values indexing the set of allowed values stored in the derived array named:
     `__discrete_values_<variable_name>`. Invalid value is stored as 0 index. 
  - `interval`: Exclusive with discrete, works only for float quantities. Defines min and max allowed values for the variable.
     The interval is given as a pair  [min, max] or triplet [min, max, unit]. Default is variable's unit. 
     E.g. to define allowed temperature range between -20C and 40C one could write:
     `interval: [-20, 40, 'degC']`

### `description` (optional)
String description of the variable. Could describe: physical name, origin of the values, measurement environment, or its use.
More formal metadata structure for the data origin are planed for future versions.

### `df_col` (optional, deprecated)
String with name of the source column in the updating data frames. Default is the variable name.
The dataset is updated through the `zf.Node.update` to which the polars or pandas Data Frame is passed.
To get the variable values the `df_col` column is taken, first interpreting the values as the `source_unit` and then 
convert them to the variable `unit`.
Will soon be deprecated in favor of Apache Air workflow conversion task.

### `source_unit` (optional, deprecated)
String or dictionary defining the unit of the source data frame column.
If not provided, the variable `unit` is assumed.
Will soon be deprecated in favor of Apache Air workflow conversion task.

### `attrs`
Dictionary of custom variable attributes.

## COORDS
The COORDS dictionary defines coordinates of the dataset stored in the node.
Each coordinate is defined by its own dictionary of properties, that includes 
all properties of variables since each coordinate is also a 1D variable. 
Therefore, only coordinate specific properties are described below.

### `composed`
List of variables forming a single tuple valued coordinate.
This allows to have, e.g. sparse set of (longitude, latitude) coordinates to form a coordinate values. 
The actual coordinated values are hashes of the tuples, while the values or the components are given by
the respecting variables. 

### `sorted`
If True, the coordinate values are always sorted in ascending order.

### `chunk_size` (optional, 1024)
Controls underlaying chunking in the ZARR store. Need more work to support parallel writes with zarr fuse.

### `step_limits`: (optional, obsolete) 
Optional control over added values in the coordinate. This has to be redesigned as it 
doesn't work in parallel updates. Zarr is not designed to appending to more coordinates than one.
The mechanism also involve interpolation which leads to loss of original values. It will be replaced by Apache Air workflow.
  - `None` : No new values allowed after the initial write.
  - `[]` : No restrictions, add all new values.
  - `[min, max]` : Restrict the step between sorted coordinate values to this interval, and then interpolate to this grid.
  -  `[min, max, unit]` : The tuple (min,max) values as given ure used to create a quantity using `unit` (which is the coordinate unit by default).
     This allows to use the same syntax for the coordinate values as for the range specification, but also allows to write a bit more
     understandable range if using different time scales. 
     E.g. one can write step_limits: [72, 126, 'minutes'] , regardless of the coordinate unit precision. 
   
 



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
