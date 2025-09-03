# ZARR FUSE Dashboard

A modern, interactive dashboard for exploring Zarr stores with advanced visualization capabilities. Built with React, TypeScript, FastAPI, and Plotly.js.

## Features

- **Tree Structure Explorer**: Navigate through Zarr store hierarchies
- **Weather Data Visualization**: Interactive plots for meteorological data
- **Advanced Plotting**: Multiple plot types including maps, time series, and 4D visualizations
- **Mock Data Plots**: Built-in sample data for testing and development
- **Responsive Design**: Modern UI with Tailwind CSS
- **Real-time Data**: Live data fetching from Zarr stores

## Project Structure

```
dashboard/
├── src/                    # Frontend source code
│   ├── components/         # React components
│   │   ├── plots/         # Plot components (WeatherPlots, MapView, TimeZoom)
│   │   ├── sidebar/       # Sidebar components (TreeView, WeatherView)
│   │   └── ui/            # UI components
│   ├── data/              # Mock data and data utilities
│   ├── types/             # TypeScript type definitions
│   └── App.tsx            # Main application component
├── backend/                # FastAPI backend
│   ├── routers/           # API endpoints (tree, weather, plot)
│   ├── services/          # Business logic (Zarr operations)
│   ├── models/            # Pydantic data models
│   └── main.py            # FastAPI application
├── public/                 # Static assets
└── package.json            # Node.js dependencies
```

## Prerequisites

- **Node.js** 18+ and **npm** 9+
- **Python** 3.8+ and **pip**
- **Zarr stores** (structure_tree.zarr, structure_weather.zarr)

## Installation and Setup

### 1. Clone and Navigate
```bash
git clone <your-repo-url>
cd app/databuk/dashboard
```

### 2. Frontend Setup
```bash
# Install Node.js dependencies
npm install

# Verify installation
npm run lint
```

### 3. Backend Setup
```bash
# Navigate to backend directory
cd backend

# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Verify installation
python -c "import fastapi, zarr, plotly; print('All dependencies installed successfully!')"
```

### 4. Zarr Store Setup
Ensure you have the required Zarr stores in your project:
- `structure_tree.zarr` - Contains file hierarchy data
- `structure_weather.zarr` - Contains meteorological data

## Running the Application

### 1. Start Backend Server
```bash
cd backend

# Activate virtual environment if not already active
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

# Start the server
python run.py
```

The backend will start on `http://localhost:8000`

**Alternative method:**
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Start Frontend Development Server
```bash
# In a new terminal, from the dashboard root directory
npm run dev
```

The frontend will start on `http://localhost:5173` (or next available port)

### 3. Access the Dashboard
Open your browser and navigate to:
- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs

## API Endpoints

### Tree Structure
- `GET /api/tree/structure` - Get complete tree hierarchy
- `GET /api/tree/file/data` - Get file data by path
- `GET /api/tree/node` - Get specific node information

### Weather Data
- `GET /api/weather/structure` - Get weather variables structure
- `GET /api/weather/variable` - Get specific variable data

### Health & Info
- `GET /` - API information
- `GET /health` - Health check
- `GET /docs` - Interactive API documentation (Swagger UI)

## Available Plot Types

### Weather Data Plots
- **Basic Plots**: Line charts, scatter plots
- **Map View**: Geographic visualization with coordinates
- **Time Zoom**: Interactive time series with zoom capabilities

### Mock Data Plots
- **Basic Plots**: Sample temperature, humidity, wind data
- **Time Zoom**: Interactive time series examples
- **Hlava Map**: Geographic mock data visualization
- **Map View**: Coordinate-based mock data
- **4 Variables**: Multi-variable mock data plots
- **Hlava Time Zoom**: Advanced time series mock data

## Development

### Frontend Development
```bash
# Start development server with hot reload
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview

# Run linting
npm run lint
```

### Backend Development
```bash
cd backend

# Start with auto-reload
python run.py

# Or with uvicorn
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Code Structure
- **Components**: React functional components with TypeScript
- **State Management**: React hooks (useState, useEffect)
- **Styling**: Tailwind CSS with custom components
- **Data Fetching**: Fetch API with async/await
- **Plotting**: Plotly.js with React wrapper

## Troubleshooting

### Common Issues

**Backend won't start:**
- Check if port 8000 is available
- Verify Python dependencies are installed
- Check virtual environment activation

**Frontend won't start:**
- Verify Node.js version (18+)
- Clear npm cache: `npm cache clean --force`
- Delete node_modules and reinstall: `rm -rf node_modules && npm install`

**Zarr store errors:**
- Verify Zarr stores exist and are accessible
- Check file permissions
- Ensure stores are valid Zarr format

**CORS errors:**
- Backend CORS is configured for localhost:5173
- Check if frontend is running on correct port

### Debug Mode
```bash
# Backend debug logging
cd backend
python run.py --debug

# Frontend development tools
# Use browser DevTools for React debugging
```

## File Locations

### Important Files
- **Main App**: `src/App.tsx`
- **Sidebar**: `src/components/sidebar/Sidebar.tsx`
- **Tree View**: `src/components/sidebar/TreeView.tsx`
- **Weather View**: `src/components/sidebar/WeatherView.tsx`
- **Plot Components**: `src/components/plots/`
- **Mock Data**: `src/data/mockPlotData.ts`
- **Backend API**: `backend/main.py`

### Configuration Files
- **Frontend**: `package.json`, `tsconfig.json`, `vite.config.ts`
- **Backend**: `requirements.txt`, `backend/core/config.py`
- **Styling**: `tailwind.config.js`, `postcss.config.cjs`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Notes

- Environment files (`.env.local`) are ignored by git
- Backend automatically detects test stores from project structure
- CORS is configured for frontend development
- Mock data plots work independently of backend

## Links

- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

---

**Happy coding!**
