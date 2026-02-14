# Zarr-fuse store variables
When opening a zarr store, the zarr-fuse uses a dictionary of variables configuring the connection or
other properties of the whole store. This configuration can vary for different instances of the same store schema.

## Environment variables
The default values of these variables are get from the root ATTRS of the schema, 
but these could be overwritten by the system environment variables named "ZF_<variable name>".
Environment variable are recommended mean to pass S3 secrets, see `S3_ACCESS_KEY` and `S3_SECRET_KEY` below.

# zf.open_store options
The `zf.open_store` function also accepts **kwargs dictionary which
have even higher priority then the environment variables.

## Store Instance Variables
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

