import requests, io

url = "https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/api/v1/tree"
csv_data = """time,temperature
2025-05-13T08:00:00,20.0
2025-05-13T09:00:00,21.5
"""
files = {"file": ("data.csv", io.BytesIO(csv_data.encode("utf-8")))}

response = requests.post(url, files=files)
print(response.status_code, response.text)
