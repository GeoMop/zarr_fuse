# ZarrFuse Ingress – Docker/OCI

Backend-only Flask service running in a container with **gunicorn**.
The image contains both the `zarr_fuse` library and the ingress server code.

---

## Build
Run **from the repository root**, so that `zarr_fuse` and ingress server sources are included in the build.

```bash
docker build -t zarr-fuse-ingress \
  -f app/databuk/ingress_server/oci/Dockerfile .
```

---

## Run (local)
The image runs with **gunicorn** on port **8000**, as user **ingress** (UID `11233`).

```bash
docker run --rm -p 8000:8000 \
  --env-file app/databuk/ingress_server/src/.env \
  zarr-fuse-ingress
```

---

## `.env` file
Keep this file **out of version control**.

```ini
S3_ACCESS_KEY=YOUR_ACCESS_KEY
S3_SECRET_KEY=YOUR_SECRET_KEY
```

You can create it quickly:

```bash
cat > app/databuk/ingress_server/src/.env <<'EOF'
S3_ACCESS_KEY=YOUR_ACCESS_KEY
S3_SECRET_KEY=YOUR_SECRET_KEY
EOF
```

---

## Quick test

```bash
curl -X POST http://localhost:8000/api/v1/tree \
  -F "file=@path/to/example.csv"
```

---

## Notes
- The container already includes `schemas/` and `endpoints_config.yaml` from the build context.
  If you want to override them without rebuilding, you may mount them as read-only volumes:
  ```bash
  -v $(pwd)/app/databuk/ingress_server/src/schemas:/ingress-server/src/schemas:ro
  -v $(pwd)/app/databuk/ingress_server/src/endpoints_config.yaml:/ingress-server/src/endpoints_config.yaml:ro
  ```
- The image uses **gunicorn** with:
  - 1 worker
  - 2 threads
  - 60s timeout
  - preload enabled
  - logs sent to stdout/stderr
