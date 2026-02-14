import shutil, numpy as np, xarray as xr, zarr, numcodecs

store_path = "vlen_utf8_demo.zarr"
shutil.rmtree(store_path, ignore_errors=True)

# 1) Start with different-length strings
arr = np.array(["a", "abc", "abcdefg"], dtype=object)
da = xr.DataArray(arr, dims="x", name="s")

# 2) Tell xarray/Zarr to use variable-length UTF-8
encoding = {"s": {"serializer": zarr.codecs.VLenUTF8Codec()}}
da.to_dataset().to_zarr(store_path, mode="w", encoding=encoding)

# 3) Inspect the raw Zarr array
root = zarr.open(store_path, mode="r+")
za = root["s"]
print("dtype:", za.dtype)                 # expect: object
print("filters:", za.filters)             # expect: [VLenUTF8()]
print("values:", list(za[:]))

# 4) Overwrite with longer strings (no truncation)
za[1] = "X"*500
za[2] = "🙂 unicode ok? — 𝛑"*2
print("after overwrite (zarr read):", list(za[:]))

# 5) Read back via xarray
ds = xr.open_zarr(store_path)
print("xarray readback:", ds["s"].values.tolist())

########################
"""
Conclusion:
- Zarr v3 supports variable length strings via VLenUTF8Codec
- Does not support other variable-length types (e.g., bytes), and there is probably no need for that
"""