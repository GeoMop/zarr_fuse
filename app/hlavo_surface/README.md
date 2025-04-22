# Colletiong weather and field soil moisture mesurement

Use ZARR fuse to compare scrapped yr.no forecast data to historical meteosat and nasa.


## Install environemnt
```
bash setup_env 
```

## Scrapper scripts

Placed in `scrappers`.

`python3 -m hlavo_surface.scrap.weather`
scrapes yr.no forecast data for a given location and time range.

`python3 -m hlavo_surface.scrap.soil DIR`
Collect CSV files with moisture measurements from DIR.
