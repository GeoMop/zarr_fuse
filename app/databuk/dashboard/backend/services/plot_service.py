import plotly.graph_objects as go
import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from typing import Tuple

logger = logging.getLogger(__name__)


def find_nearest_time(times_array: np.ndarray, target_time) -> pd.Timestamp:
    """
    Find the nearest time in array to the target time.
    
    Args:
        times_array: Array of unique times
        target_time: Target time to match
        
    Returns:
        Nearest timestamp in the array
    """
    times_series = pd.Series(times_array)
    if not isinstance(target_time, (pd.Timestamp, np.datetime64)):
        target_time = pd.to_datetime(target_time)
    nearest_idx = (times_series - target_time).abs().idxmin()
    return times_series.iloc[nearest_idx]


def detect_and_fix_coordinates(
    lats: np.ndarray, 
    lons: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Detect and fix coordinate issues:
    - Swapped coordinates (lat/lon)
    - Radian vs Degree format
    
    Args:
        lats: Latitude values
        lons: Longitude values
        
    Returns:
        Tuple of (fixed_lats, fixed_lons)
    """
    valid_mask = np.isfinite(lats) & np.isfinite(lons) & (np.abs(lats) <= 360)
    
    if np.any(valid_mask):
        mean_lat = np.mean(lats[valid_mask])
        mean_lon = np.mean(lons[valid_mask])
        
        # Swap detection: European context heuristic
        # If mean latitude is low (<30) but mean longitude is high (>30),
        # coordinates might be swapped
        if mean_lat < 30 and mean_lon > 30:
            logger.debug(f"Detected swapped coordinates. Lat={mean_lat:.1f}, Lon={mean_lon:.1f}")
            lats, lons = lons, lats
            
        # Radian detection: if max value is small, likely in radians
        max_val = np.max(np.abs(lats[valid_mask]))
        if max_val < 1.6:
            logger.debug("Detected radians format. Converting to degrees.")
            lats = np.degrees(lats)
            lons = np.degrees(lons)
    
    return lats, lons


def generate_map_figure(df: pd.DataFrame, time_point=None, lat_col='latitude', lon_col='longitude', time_col='time'):
    """
    Generate a Plotly map figure based on the provided DataFrame and time point.
    
    Args:
        df: DataFrame containing geographic and temporal data
        time_point: Specific time to filter (optional, defaults to earliest)
        lat_col: Column name for latitude
        lon_col: Column name for longitude
        time_col: Column name for time
        
    Returns:
        Dictionary representation of Plotly figure
    """
    logger.debug(f"Generating map figure. Time: {time_point}")
    try:
        # Ensure time column is datetime
        if not np.issubdtype(df[time_col].dtype, np.datetime64):
            df[time_col] = pd.to_datetime(df[time_col])
            
        # 1. Filter by Time
        if time_point is None:
            selected_time = df[time_col].min()
        else:
            selected_time = find_nearest_time(df[time_col].unique(), time_point)
            
        logger.debug(f"Selected time: {selected_time}")
        
        current_df = df[df[time_col] == selected_time].copy()
        
        if current_df.empty:
            logger.warning(f"No data found for selected time: {selected_time}")
            return go.Figure().to_dict()

        # 2. Coordinate Handling (Swap & Radian Detection)
        lats = current_df[lat_col].values.astype(float)
        lons = current_df[lon_col].values.astype(float)
        
        # Fix coordinate issues
        lats, lons = detect_and_fix_coordinates(lats, lons)
                
        # 3. Create Plotly Figure
        logger.debug("Creating Plotly figure with map markers")
        fig = go.Figure()
        
        # Prepare hover text
        # Exclude coordinate and time columns from hover
        hover_cols = [c for c in current_df.columns if c not in [lat_col, lon_col, time_col]]
        hover_texts = []
        for _, row in current_df.iterrows():
            lines = [
                f"{col}: {row[col]:.2f}" if isinstance(row[col], (int, float)) else f"{col}: {row[col]}"
                for col in hover_cols
            ]
            hover_texts.append("<br>".join(lines))
            
        fig.add_trace(go.Scattermap(
            lat=lats.tolist(),
            lon=lons.tolist(),
            mode='markers',
            marker=dict(
                size=10,
                opacity=0.8,
                color='red',
            ),
            text=hover_texts,
            hoverinfo='text',
            name='Data'
        ))

        # 4. Layout & Auto Focus
        center_lat = 0
        center_lon = 0
        
        plot_mask = np.isfinite(lats) & (np.abs(lats) <= 90)
        if np.any(plot_mask):
            center_lat = float(np.mean(lats[plot_mask]))
            center_lon = float(np.mean(lons[plot_mask]))
            
        fig.update_layout(
            template=None,
            map=dict(
                style="open-street-map",
                center=dict(lat=center_lat, lon=center_lon),
                zoom=5
            ),
            margin=dict(l=0, r=0, t=0, b=0),
            height=600,
            showlegend=False
        )
        
        return fig.to_dict()
        
    except Exception as e:
        logger.error(f"Error generating map figure: {e}")
        raise
