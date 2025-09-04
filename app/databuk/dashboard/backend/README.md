# ZARR FUSE Dashboard Backend

Modular FastAPI backend for exploring Zarr stores and building tree structures.

## Architecture

```
backend/
├── core/           # Configuration and utilities
├── models/         # Pydantic response models
├── services/       # Business logic (Zarr operations)
├── routers/        # HTTP API endpoints
├── main.py         # FastAPI application
├── run.py          # Server startup script
└── requirements.txt # Python dependencies
```

## Setup

1. **Install dependencies:**
   ```bash
   cd backend
   pip install -r requirements.txt
   ```

2. **Run the server:**
   ```bash
   python run.py
   ```
   
   Or directly with uvicorn:
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

## API Endpoints

### Tree Structure
- `GET /api/tree/structure` - Get complete tree hierarchy
- `GET /api/tree/node` - Get specific node information
- `GET /api/tree/store/summary` - Get store summary

### Health & Info
- `GET /` - API information
- `GET /health` - Health check
- `GET /docs` - Interactive API documentation (Swagger UI)

## Configuration

The backend automatically detects test stores from the project structure:
- **structure_tree.zarr**: Located at `zarr_fuse/test/workdir/structure_tree.zarr`
- **CORS**: Configured for frontend development (`localhost:5173`)

## Data Flow

1. **S3Service**: Opens and explores Zarr stores from S3
2. **S3Router**: Exposes HTTP endpoints for S3 operations
3. **Frontend**: Consumes data for sidebar rendering and variable display

## Testing

Test the API:
```bash
# Health check
curl http://localhost:8000/health

# Get tree structure
curl http://localhost:8000/api/tree/structure

# Get specific node
curl "http://localhost:8000/api/tree/node?path=root"
```

## Next Steps

- [ ] Add weather data endpoints
- [ ] Implement data sampling for plotting
- [ ] Add authentication/authorization
- [ ] Implement caching for large trees
- [ ] Add metrics and monitoring
