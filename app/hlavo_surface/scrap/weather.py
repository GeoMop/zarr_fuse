"""
Scraping data from yr.no for Uhelna, Czech Republic
- all configured through uhelna_data.yaml
- contains
"""

import numpy as np
import polars as pl

import zarr_fuse

import hlavo_surface.inputs as inputs
work_dir = inputs.work_dir
from . import generic, yr_no

def grid_points(grid_min, grid_max, grid_step):
    #min = (50.840000, 14.850000)
    #max = (50.8900000, 14.9600000)

    step = 1e-2 # about 1000m; approximately resolution of the yr.no service
    lon_rng = np.arange(grid_min[0], grid_max[0], grid_step[0])
    lat_rng = np.arange(grid_min[1], grid_max[1], grid_step[1])
    lon, lat =  np.meshgrid(lon_rng, lat_rng)
    return lon.flatten(), lat.flatten()

def sensor_locations(source_path):
    raw_df = pl.read_csv(source_path, has_header=True)
    lonlat = raw_df['GPS']
    lon, lat = zip(*[row.split(',') for row in lonlat if row is not None])
    lon = np.array(lon, dtype=float)
    lat = np.array(lat, dtype=float)
    return lon, lat


def location_df(attrs_dict):
    array_get = lambda key: np.array(attrs_dict[key], dtype=float)
    grid_lon, grid_lat = grid_points(array_get('grid_min'), array_get('grid_max'), array_get('grid_step'))
    grid_flag = np.ones_like(grid_lon, dtype=bool)
    sen_lon, sen_lat = sensor_locations(inputs.input_dir / attrs_dict['location_file'])
    grid_flag_sen = np.zeros_like(sen_lon, dtype=bool)
    lon = np.concatenate([grid_lon, sen_lon])
    lat = np.concatenate([grid_lat, sen_lat])
    grid_flag = np.concatenate([grid_flag, grid_flag_sen])
    df = pl.DataFrame({'lon': lon, 'lat': lat, 'grid': grid_flag})
    return df

# def create_zarr(zarr_path: Path):
#     grid_lon, grid_lat = grid_points()
#     grid_flag = np.ones_like(grid_lon, dtype=bool)
#     sen_lon, sen_lat = sensor_locations()
#     grid_flag_sen = np.zeros_like(sen_lon, dtype=bool)
#     lon = np.concatenate([grid_lon, sen_lon])
#     lat = np.concatenate([grid_lat, sen_lat])
#     grid_flag = np.concatenate([grid_flag, grid_flag_sen])
#     loc_struct = pl.DataFrame(dict(lon=lon, lat=lat)).to_struct()
#     df = pl.DataFrame({
#         "time": datetime.now(),
#         'loc': loc_struct,
#         'grid': grid_flag})
#     indices = {
#         'time': 0,
#         'loc': 200,
#     }
#     # TODO: grid as non-dim coordinate
#     create(zarr_path, df, indices)



def loc_forecast(scrapping_fn, cache, location):
    lon, lat, grid = [location[k] for k in ['lon', 'lat', 'grid']]
    df = scrapping_fn(cache, lon, lat)
    df = df.with_columns(
        longitude=lon,
        latitude=lat,
        grid_domain=grid
    )
    print(df)
    return df

def update_meteo(node, scrapping_fn, df_locs: pl.DataFrame):
    yr_no_node = node
    loc_df = df_locs #.filter(pl.col('grid') == 1)

    html_cache_flag = node.dataset.attrs['update']['html_cache']
    cache = generic.create_http_cache(work_dir / 'http_cache.sqlite', html_cache_flag)
    loc_dfs = [loc_forecast(scrapping_fn, cache, loc) for loc in loc_df.iter_rows(named=True)]
    # Concatenate all DataFrames
    final_df = pl.concat(loc_dfs)

    # df = final_df
    #
    # # Total number of unique locations
    # total_locs = df["location"].n_unique()
    # # Count unique 'loc' per 'time'
    # time_counts = df.group_by("date_time").agg(pl.col("location").n_unique().alias("loc_count"))
    # # Find first time where all locations are present
    # first_complete_time = time_counts.filter(pl.col("loc_count") == total_locs)["date_time"].min()
    #
    # # Filter only the first time and select 'air_temperature' for all 'loc' values
    # result = df.filter(pl.col("date_time") == first_complete_time)
    #
    # # Print result
    # print(result)

    yr_no_node.update(final_df)



def main():
    schema = zarr_fuse.schema.deserialize(inputs.surface_schema_yaml)
    df_locs = location_df(schema.ds.ATTRS)

    root_node = zarr_fuse.open_storage(schema, workdir = work_dir)
    update_meteo(   root_node['yr.no'], yr_no.get_3day_forecast, df_locs)

if __name__ == '__main__':
    main()