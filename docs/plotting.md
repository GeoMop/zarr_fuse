# Plotting

## Dataset 5 principal dimensions

- **Time** 1D, sortable, multiscale, daytime/period  
- **Location** non-sortable, both discrete and coordinates, multiscalle, more then 3D  
- **Realization** samples  
- **Source** environment, device, (input parameters … meaning of realization?)  
- **Quantity** 

## Plot types

1. **Storage dashboard page**
   * Sources contributions  
   * Log messages/errors and warnings \-\> issues, communication threads (outsourced)  
   
2. **Storage tree view**
   * Tree of nodes  
   * Node quantities, their coordinate span  
   * Output: selection of one or more quantities  
   * ? application to a particular plot or use as a config dialog/panel that is part of any  
     Plot (expandable legend)

1. **Timedependent**    
   * Single quantity dependent on time  
   * Combine a small number of different variants: location, realization, source  
   * Histogram over realizations  
   * Mean \+ variance/CF intervals  
   * Boxplot colored (at most two quantities)  
   * Zoomable axis by mouse wheel, or both in the corner  
   * Guidelines for a pointer inside.  
   * Multiscale view (configurable, fixed relations)  
   * Output: time point, selected zoom range, selected variant \[location, source, realization\]  
   * Population selection: realizations, space  
   * Quantity log scale  
     

2. **Map view**

   * Colormap plot dependence on two location components.  
   * Enable contours (automatic \+ custom)  
   * An optional discrete locations plot using colored icons.  
   * Icons vs. interpolated field  
   * Automatic suggestion of the colormap associated with quantity, use consistently across plots.  
   * Mouse wheel zoom  
   * Output: location selection (discrete and 2D), location range 

3. **Correlation pairs**
   * for selected small number of $N$ quantities
   * N x N plot matrix
   * color map of 2d density or scatter plot


3. **Future ideas**
   - Correlation matrix
   - interactive 3D scene
   


----

Analysis types:

* Single quantity  \- as a function of different coordinates or their combinations  
* Two or more quantities \- correlations, relations on subsets of the parametric space.  
* For these, we want to provide specific views of quantity as a function of the coordinate to visualize large datasets. 

* Storage overview: storage tree view, generic dataset overview  
* Specialized view for long time series with effects on various scales (year, month, week, day, smaller)  
  * Single quantity at different locations in a single plot  
  * Different quantities  
  * Stochastic distribution for each time  
  * Periodic \- more time periods in a single plot  
  * Separate synchronized plots for different scales  
* Map/space view \- overview of spatial distribution of measurements/simulation results  
* Stochastic view (  
* selection mechanism \-\> named variable group.  
* Overview of dashboard and visualization tools.  
*  visualization.  
* Time series.  
* Statistical samples.  
* Position clouds
