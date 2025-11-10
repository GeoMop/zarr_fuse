#import zarr
import s3fs

import asyncio
import fsspec



def loop_issue_fsspec_sync():
    bucket_name = "test-zarr-storage"
    s3_key = "4UD5K2LCS5ZU8GHL5TJS"
    s3_secret = "VztZ2COyVsgADEGbftd1Zt6XdtN6QXwOhSfEKT0Y"
    endpoint_url = "https://s3.cl4.du.cesnet.cz"
    root_path = f"{bucket_name}/test.zarr"

    storage_options = dict(
        key=s3_key,
        secret=s3_secret,
        #asynchronous=True,
        client_kwargs=dict(endpoint_url=endpoint_url),
        config_kwargs={
            "s3": {"addressing_style": "path"},
        },
    )
    fs = fsspec.filesystem('s3', **storage_options)
    try:
        fs.rm(root_path, recursive=True, maxdepth=None)
    except FileNotFoundError:
        pass  # Ignore if the S3 path does not exist


def loop_issue_fsspec_async():
    bucket_name = "test-zarr-storage"
    s3_key = "4UD5K2LCS5ZU8GHL5TJS"
    s3_secret = "VztZ2COyVsgADEGbftd1Zt6XdtN6QXwOhSfEKT0Y"
    endpoint_url = "https://s3.cl4.du.cesnet.cz"
    root_path = f"{bucket_name}/test.zarr"

    storage_options = dict(
        key=s3_key,
        secret=s3_secret,
        asynchronous=True,
        client_kwargs=dict(endpoint_url=endpoint_url),
        config_kwargs={
            "s3": {"addressing_style": "path"},
        },
    )
    loop = asyncio.new_event_loop()
    fs = fsspec.filesystem('s3', **storage_options)

    # # loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(fs._rm(root_path, recursive=True, maxdepth=None))
    except FileNotFoundError:
        pass  # Ignore if the S3 path does not exist
    finally:
        loop.close()


def loop_issue_s3fs_sync():
    bucket_name = "test-zarr-storage"
    s3_key = "4UD5K2LCS5ZU8GHL5TJS"
    s3_secret = "VztZ2COyVsgADEGbftd1Zt6XdtN6QXwOhSfEKT0Y"
    endpoint_url = "https://s3.cl4.du.cesnet.cz"
    root_path = f"{bucket_name}/test.zarr"

    storage_options = dict(
        key=s3_key,
        secret=s3_secret,
        asynchronous=True,
        client_kwargs=dict(endpoint_url=endpoint_url),
        config_kwargs={
            "s3": {"addressing_style": "path"},
        },
    )
    fs = s3fs.S3FileSystem(**storage_options)

    try:
        fs._rm(root_path, recursive=True, maxdepth=None)
    except FileNotFoundError:
        pass  # Ignore if the S3 path does not exist
    # loop = asyncio.new_event_loop()
    # loop = asyncio.get_event_loop()
    # try:
    #     loop.run_until_complete(fs._rm(root_path, recursive=True, maxdepth=None))
    # finally:
    #     loop.close()


def main():
    bucket_name = "test-zarr-storage"
    s3_key = "4UD5K2LCS5ZU8GHL5TJS"
    s3_secret = "VztZ2COyVsgADEGbftd1Zt6XdtN6QXwOhSfEKT0Y"
    storage_options = dict(
        key=s3_key,
        secret=s3_secret,
        asynchronous=True,
        client_kwargs=dict(
            endpoint_url="https://s3.cl4.du.cesnet.cz"),
        config_kwargs={
            "s3": {"addressing_style": "path"},
        },
    )

    fs = fsspec.filesystem('s3', **storage_options)
    root_path = f"{bucket_name}/test.zarr"
    store = zarr.storage.FsspecStore(fs, path=root_path)
    print(f"listing supported: {store.supports_listing}")

    root = zarr.open_group(store, path="", mode='r')

    root = zarr.open_group(store=store, mode='r')

    fs = s3fs.S3FileSystem(**storage_options)
    #s3_store = zarr.storage.FsspecStore.from_url(root_path, storage_options)
    s3_store = zarr.storage.FsspecStore(fs, path=f"{bucket_name}/test.zarr")

    #fs = s3_store.fs  # async S3FileSystem

    # async open for writing
    # f = fs._open(f"{bucket_name}/test.out", "wb")
    # with f:
    #     f.write(b"Success write!")

    # async open_group -- make sure your zarr version supports this
    # If not, install zarr>=2.17
    zarr.open_group(store).store
    root_group = zarr.open_group(s3_store, path="", mode='r')
    sub_groups = list(root_group.groups())
    print(f"Subgroups in root: {sub_groups}")
    # try:
    #     root_group = await zarr.asynchronous.open_group(s3_store, path="", mode='r')
    # except AttributeError:
    #     # Fallback for older zarr: use zarr.api.asynchronous
    #     root_group = await zarr.api.asynchronous.open_group(s3_store, path="", mode='r')

    # Do something with root_group, e.g.
    #print(await root_group.attrs.async_get())

#main()

#loop_issue_s3fs_sync()
#loop_issue_fsspec_sync()
loop_issue_fsspec_async()