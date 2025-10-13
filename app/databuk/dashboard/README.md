# ZARR FUSE Dashboard

[release deploy](https://zarr-fuse-dashboard.dyn.cloud.e-infra.cz/)

A modern, interactive dashboard for exploring Zarr stores on S3 with real-time data visualization. 

Built with React, TypeScript, FastAPI, and designed for generic Zarr store structures.

## Features

- **Generic S3 Zarr Store Support**: Works with any S3-hosted Zarr store structure
- **Interactive Tree Explorer**: Navigate through hierarchical Zarr groups and arrays
- **Variable Data Viewer**: Accordion-style variable display with sample data
- **Real-time Store Connection**: Live S3 connection with status indicators
- **Progress Bar & Auto-reload**: Configurable reload intervals with visual progress
- **Error Handling**: Comprehensive error display and debugging
- **YAML Configuration**: Flexible endpoint configuration via YAML files
- **Responsive Design**: Modern UI with Tailwind CSS

## Architecture

### Frontend (React + TypeScript)
```
src/
├── components/
│   ├── sidebar/           # Store navigation and tree view
│   └── App.tsx           # Main application with variable display
├── types/                # TypeScript type definitions
└── index.css            # Tailwind CSS styles
```

### Backend (FastAPI + Python)
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

## Quick Start

### Prerequisites
- **Python 3.8+** with zarr_fuse library installed
- **Node.js 18+** and npm
- **S3 credentials** and access to Zarr stores

### 1. Environment Setup
```bash
# Clone repository
git clone <repo-url>
cd zarr_fuse/app/databuk/dashboard

npm install
```

### 2. Backend Setup
```powershell
cd backend
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -e .[dev]
```

### 3. Configuration

Create `.env` file in `backend/` (copy from `backend/env.example`):
```ini
# S3 Configuration (placeholders)
S3_BUCKET_NAME=your_bucket_name
S3_ACCESS_KEY=your_access_key_here
S3_SECRET_KEY=your_secret_key_here
S3_ENDPOINT_URL=https://s3.example.com
S3_ADDRESSING_STYLE=path
```

Configure `backend/config/endpoints.yaml`:
```yaml
"your_endpoint_name":
  Reload_interval: 300  # seconds
  Schema_file: "schemas/your_schema.yaml"
  STORE_URL: "s3://${S3_BUCKET_NAME}/path/to/your/store.zarr"
  S3_ENDPOINT_URL: "${S3_ENDPOINT_URL}"
  S3_access_key: "${S3_ACCESS_KEY}"
  S3_secret_key: "${S3_SECRET_KEY}"
  S3_region: "us-east-1"
  S3_use_ssl: true
  S3_verify_ssl: true
  Description: "Your store description"
  Store_type: "zarr"
  Version: "1.0.0"
```

### 4. Run Application
```powershell
# Terminal 1: Start backend
cd backend
python run.py

# Terminal 2: Start frontend
cd ..  # back to dashboard root
npm run dev
```

Access at:
- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

## Usage

### Store Navigation
1. **Store Name**: Click store name to view root-level variables
2. **Groups**: Click group names to view nested variables
3. **Variables**: Click variables to expand and view data details

### Status Indicators
- **Green**: Store connected and operational
- **Yellow**: Loading or connecting
- **Red**: Connection error or issues

### Progress Bar
- Shows time since last reload
- Click to force immediate reload
- Auto-reloads based on `Reload_interval`

## Supported Store Structures

### Tree Structure
```
store.zarr/
├── root variables (temperature, time, etc.)
├── child_1/
│   ├── variables
│   └── child_3/
│       └── variables
└── child_2/
    └── variables
```

### Weather Structure (yr.no style)
```
store.zarr/
├── root variables (lat, lon, etc.)
└── yr.no/
    ├── air_pressure
    ├── temperature
    └── other weather variables
```

### Generic Structure
Works with any Zarr group/array hierarchy!

## API Endpoints

### Configuration
- `GET /api/config/endpoints` - Get all endpoints

### S3 Operations
- `GET /api/s3/structure` - Get store structure
- `GET /api/s3/node/{store_name}/{node_path}` - Get node details
- `GET /api/s3/variable/{store_name}/{variable_path}` - Get variable data
- `GET /api/s3/status` - Get S3 connection status

## Development

### Code Structure
- **Generic S3 handling**: Works with any Zarr store structure
- **Error resilient**: Comprehensive error handling and display
- **Configurable**: YAML-based configuration system
- **Scalable**: Clean separation of concerns

### Key Components
- **S3Service**: Core S3 and Zarr operations
- **ConfigManager**: YAML configuration management
- **Sidebar**: Store navigation and status display
- **App**: Main application with variable viewer

### Testing Different Stores
1. Update `STORE_URL` in `endpoints.yaml`
2. Restart backend
3. Frontend automatically adapts to new structure

## Recent Improvements

### Completed Features
- Generic Zarr store support (any structure)
- Root prefix handling (Zarr-native paths)
- NaN value JSON serialization
- Accordion-style variable display
- Store-level clickable navigation
- Code cleanup and refactoring
- Multi-store testing capability

### Current Status
- Fully functional with multiple store types
- Clean, maintainable codebase
- Ready for production use

## Troubleshooting

### Common Issues

**S3 Connection Errors:**
- Verify credentials in `.env`
- Check S3 endpoint URL
- Ensure bucket/store exists

**Backend Won't Start:**
- Check if zarr_fuse is installed: `pip list | grep zarr`
- Verify port 8000 is available
- Check Python version (3.8+)

**Frontend Issues:**
- Clear npm cache: `npm cache clean --force`
- Reinstall dependencies: `rm -rf node_modules && npm install`
- Check Node.js version (18+)

**Store Structure Issues:**
- Verify store is valid Zarr format
- Check S3 permissions
- Try different STORE_URL paths

## File Locations

### Configuration
- **S3 Credentials**: `backend/.env`
- **Endpoint Config**: `backend/config/endpoints.yaml`
- **Dependencies**: `backend/pyproject.toml`, `package.json`

### Core Files
- **Main App**: `src/App.tsx`
- **Sidebar**: `src/components/sidebar/Sidebar.tsx`
- **S3 Service**: `backend/services/s3_service.py`
- **API Routes**: `backend/routers/s3.py`

## Contributing

1. Follow existing code patterns
2. Test with multiple store structures
3. Update documentation
4. Ensure error handling is comprehensive

---

**Built for the Zarr community**
