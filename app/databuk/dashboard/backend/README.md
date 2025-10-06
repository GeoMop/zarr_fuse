# ZARR FUSE Dashboard Backend

Modular FastAPI backend for exploring Zarr stores on S3. Now packaged as a standalone Python project with its own `pyproject.toml` and `.env` inside `backend/`.

## Architecture

```
backend/
├── core/            # Configuration and utilities
├── services/        # Business logic (Zarr operations)
├── routers/         # HTTP API endpoints (s3, logs, config)
├── config/          # YAML endpoint configs (endpoints.yaml)
├── main.py          # FastAPI application
├── run.py           # Server startup script
├── pyproject.toml   # Packaging and dependencies
└── env.example      # Example env vars (copy to .env)
```

## Setup

1. Create and activate a virtual environment (Windows PowerShell):
   ```powershell
   cd app/databuk/dashboard/backend
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

2. Install package (editable) with dev extras:
   ```powershell
   pip install -e .[dev]
   ```

3. Configure environment variables:
   - Copy `env.example` to `.env`
   - Fill `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ENDPOINT_URL`, `S3_BUCKET_NAME`, etc.

4. Run the server:
   ```powershell
   python run.py
   ```
   Or directly with uvicorn:
   ```powershell
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

## API Endpoints

### Configuration
- `GET /api/config/endpoints` - List configured endpoints

### S3 Operations
- `GET /api/s3/status` - Connection status
- `GET /api/s3/structure` - Current store structure
- `GET /api/s3/node/{store_name}/{node_path}` - Node details
- `GET /api/s3/variable/{store_name}/{variable_path}` - Variable data

### Logs
- `GET /api/logs` - Backend warnings and errors from latest log

### Health & Info
- `GET /` - API information
- `GET /health` - Health check
- `GET /docs` - Swagger UI

## Configuration

Environment variables are loaded only from `backend/.env`.

- `.env`: S3 credentials and settings (see `env.example`)
- `config/endpoints.yaml`: defines STORE_URL and S3 options per endpoint
- CORS: enabled for `http://localhost:5173`

## Data Flow

1. S3Service: Opens and explores Zarr stores from S3
2. S3Router: Exposes HTTP endpoints for S3 operations
3. LogsRouter: Exposes backend logs to the UI
4. Frontend: Consumes data for tree and variable views

## Testing

```powershell
# Health
curl http://localhost:8000/health

# Structure
curl http://localhost:8000/api/s3/structure

# Logs
curl http://localhost:8000/api/logs
```

## Notes

- NaN/Infinity are serialized as strings for JSON compliance
- Uses Zarr FsspecStore and list_dir for S3-backed stores
