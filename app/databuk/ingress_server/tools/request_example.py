import requests

url = "https://zarr-fuse-ingress-server-latest.dyn.cloud.e-infra.cz/api/v1/tree"

csv_data = """time,temperature
2025-05-13T08:00:00,20.0
2025-05-13T09:00:00,21.5
"""

headers = {"Content-Type": "text/csv"}

r = requests.post(url, headers=headers, data=csv_data.encode("utf-8"), auth=("test", "test"))

print(r.status_code, r.text)
