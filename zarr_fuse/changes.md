# Changes 0.2.0
- support for datetime 'unit' 
- unit conversion 'source_unit' key
- update control through 'merge' key
- key 'merge.step_limits' allows to constrain addition of the new coordinate values
- coordinates within exisitng ranges are not added but values are interpolated to the  
    existing coordinates
- fixed logic in zarr write modes