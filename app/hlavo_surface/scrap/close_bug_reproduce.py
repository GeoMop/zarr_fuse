"""
Scraping data from yr.no for Uhelna, Czech Republic
- all configured through uhelna_data.yaml
- contains
"""
import time

import zarr_fuse

import hlavo_surface.inputs as inputs

work_dir = inputs.work_dir
import polars


"""
RuntimeError: Task <Task pending name='Task-9410' coro=<_runner() running at /home/jb/workspace/zarr_fuse/venv/lib/python3.12/site-packages/fsspec/asyn.py:56> cb=[_chain_future.<locals>._call_set_state() at /usr/lib/python3.12/asyncio/futures.py:394]> got Future <Task pending name='Task-9411' coro=<_wait_for_close() running at /home/jb/workspace/zarr_fuse/venv/lib/python3.12/site-packages/aiohttp/connector.py:138> wait_for=<_GatheringFuture pending cb=[Task.task_wakeup()]>> attached to a different loop

- happend at the very end of the script
- happens after simple zarr_fuse.open_store
- harmless, but annoying
"""

schema = zarr_fuse.schema.deserialize(inputs.surface_schema_yaml)
df = polars.read_csv("weather_table.csv")
with zarr_fuse.open_store(schema, workdir=work_dir) as root_node:
    print('Update')
    time.sleep(1)
    #root_node['yr.no'].update(df)
    print('Finalize')


