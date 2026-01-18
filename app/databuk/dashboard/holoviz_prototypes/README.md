# HoloViz Dashboard Prototype

A prototype dashboard built with Panel and HoloViz ecosystem to demonstrate advanced data visualization capabilities as an alternative to the React-based dashboard.

## Overview

This prototype demonstrates:
- **Resizable Layout**: GoldenLayout integration for drag-and-drop window management
- **Linked Visualizations**: Interactive scatter and line plots with selection linking
- **Geographic Maps**: Multi-layer maps using GeoViews (tiles + overlay + scatter)
- **Sidebar Controls**: Dashboard-style control panel matching the main project design

## Features

### 1. Control Panel (Left Sidebar)
- Store name selector with multiple options
- Store URL and connection information display
- Service status indicator with live updates
- Hierarchical data structure tree view with clickable nodes
- Reload data functionality

### 2. Interactive Plots (Top Panels)
- **Scatter Plot (Top Left)**: X vs Y coordinates colored by temperature
  - Click points to select them
  - Hover for detailed information
- **Time Series (Top Right)**: Temperature over time
  - Linked selection shows red markers for selected scatter points
  - Synchronized with scatter plot interactions

### 3. Geographic Map (Bottom Left)
- OpenStreetMap background tiles
- Semi-transparent overlay layer
- Scatter points with color-coded values
- Interactive pan and zoom
- Hover tooltips for data points

## Requirements

### Python Environment
- Python 3.8+
- Virtual environment recommended

### Dependencies
Install all required packages from the main project's `backend/pyproject.toml`:

```bash
cd c:\Users\fatih\Documents\GitHub\zarr_fuse\app\databuk\dashboard\backend
pip install -e .
```

Or install individually:
```bash
pip install panel>=1.8 holoviews>=1.19 geoviews>=1.12 bokeh>=3.8 numpy pandas
```

## Installation

1. **Activate Virtual Environment**:
   ```powershell
   & C:\Users\fatih\Documents\GitHub\zarr_fuse\app\databuk\dashboard\backend\venv\Scripts\Activate.ps1
   ```

2. **Navigate to Project Directory**:
   ```powershell
   cd C:\Users\fatih\Documents\GitHub\zarr_fuse\app\databuk\dashboard\holoviz_prototypes
   ```

3. **Verify Dependencies** (optional):
   ```powershell
   pip list | Select-String "panel|bokeh|holoviews|geoviews"
   ```

## Running the Application

### Standard Launch (Port 5006)
```powershell
panel serve app.py --show
```

The application will automatically open in your default browser at `http://localhost:5006/app`

### Custom Port
```powershell
panel serve app.py --show --port 5173
```

### Development Mode (Auto-reload on changes)
```powershell
panel serve app.py --show --autoreload
```

### Without Opening Browser
```powershell
panel serve app.py
```
Then manually navigate to `http://localhost:5006/app`

## Project Structure

```
holoviz_prototypes/
├── app.py              # Main application with layout and visualizations
├── mock_data.py        # Mock data generation functions
├── README.md           # This file
└── __pycache__/        # Python cache (auto-generated)
```

### File Descriptions

**app.py**
- GoldenLayout configuration for resizable panes
- Control panel UI components
- Scatter plot, line plot, and map visualizations
- Linked selection implementation
- Panel template setup

**mock_data.py**
- `generate_timeseries_data()`: Creates time-series with X, Y, temperature
- `generate_geographic_data()`: Generates lat/lon points for map
- `get_overlay_bounds()`: Returns overlay rectangle coordinates

## Key Technologies

- **Panel**: High-level dashboard framework
- **HoloViews**: Declarative data visualization
- **GeoViews**: Geographic data visualization
- **Bokeh**: Interactive plotting backend
- **GoldenLayout**: Resizable window management
- **jQuery**: Required for GoldenLayout

## Customization

### Changing Mock Data
Edit `mock_data.py` to modify:
- Number of data points
- Date ranges
- Geographic bounds
- Value ranges

### Adjusting Layout
In `app.py`, modify the GoldenLayout `config` object:
- Change panel widths (`width` property)
- Adjust panel arrangement (row/column structure)
- Add/remove panes

### Styling
Update CSS in the template section or individual component styles:
- Color schemes (currently dark theme)
- Font sizes
- Spacing and padding

## Troubleshooting

### Import Errors
```
ModuleNotFoundError: No module named 'panel'
```
**Solution**: Activate the correct virtual environment and install dependencies

### Port Already in Use
```
OSError: [Errno 48] Address already in use
```
**Solution**: Use a different port with `--port 5007` or stop the conflicting process

### Yellow Underlines in VS Code
```
Import "panel" could not be resolved
```
**Solution**: Select the correct Python interpreter in VS Code:
- Press `Ctrl+Shift+P`
- Type "Python: Select Interpreter"
- Choose: `.\app\databuk\dashboard\backend\venv\Scripts\python.exe`

## Comparison with React Dashboard

| Feature | React Dashboard | HoloViz Prototype |
|---------|----------------|-------------------|
| Language | TypeScript | Python |
| Framework | React + Vite | Panel + Bokeh |
| Charts | Plotly.js | HoloViews/Bokeh |
| Maps | Plotly Mapbox | GeoViews + OSM |
| Layout | CSS Grid | GoldenLayout |
| Port | 5173 | 5006 |

## Next Steps

### Potential Enhancements
1. Connect to real ZARR data sources
2. Add real-time data updates
3. Implement depth filtering
4. Add more visualization types
5. Create exportable dashboards
6. Add user authentication

### Integration Path
- Replace mock data with backend API calls
- Connect to existing FastAPI endpoints
- Implement store selection logic
- Add error handling and loading states

## Resources

- [Panel Documentation](https://panel.holoviz.org/)
- [HoloViews Documentation](https://holoviews.org/)
- [GeoViews Documentation](https://geoviews.org/)
- [GoldenLayout Documentation](https://golden-layout.com/)
- [Bokeh Documentation](https://docs.bokeh.org/)

## Contact

For questions or issues related to this prototype, refer to the main ZARR FUSE project documentation or contact the development team.
