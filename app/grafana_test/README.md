# Grafana test

Grafana provides a dashborad visualization tool based on TypeScript and React.

Dashboards could be created interactively, but configuration could be saved to JSON
allowing versioning.

Support for datasources: SQL, Prometheus (dict of timeseries), InfluxDB (multidimensional time series data)

## Sources
This folder contains a working Docker image with anonymous access
and minimalistic dashboard.

build:
`docker build -t my-grafana .`

deploy:
`docker run -it -p 3000:3000 my-grafana`
