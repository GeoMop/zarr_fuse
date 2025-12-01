import plotly.graph_objects as go
import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def generate_map_figure(df: pd.DataFrame, time_point=None, lat_col='latitude', lon_col='longitude', time_col='time'):
    """
    Generates a Plotly map figure based on the provided DataFrame and time point.
    """
    print(f"DEBUG: Generate map figure called. Time: {time_point}")
    try:
        # Ensure time column is datetime
        if not np.issubdtype(df[time_col].dtype, np.datetime64):
            df[time_col] = pd.to_datetime(df[time_col])
            
        # 1. Filter by Time
        if time_point is None:
            selected_time = df[time_col].min()
        else:
            selected_time = pd.to_datetime(time_point)
            # Find nearest time
            unique_times = df[time_col].unique()
            # Sort unique times to ensure correct nearest search
            unique_times.sort()
            
            # Find nearest index
            # Using searchsorted for efficiency or simple absolute difference
            # Converting to Series for easy abs diff
            times_series = pd.Series(unique_times)
            nearest_idx = (times_series - selected_time).abs().idxmin()
            selected_time = times_series.iloc[nearest_idx]
            
        print(f"DEBUG: Generating map for time: {selected_time}")
        
        current_df = df[df[time_col] == selected_time].copy()
        
        if current_df.empty:
            print("WARNING: No data found for the selected time.")
            return go.Figure().to_dict()

        # 2. Coordinate Handling (Swap & Radian Detection)
        lats = current_df[lat_col].values.astype(float)
        lons = current_df[lon_col].values.astype(float)
        
        # Clean invalid data
        valid_mask = np.isfinite(lats) & np.isfinite(lons) & (np.abs(lats) <= 360)
        
        if np.any(valid_mask):
            mean_lat = np.mean(lats[valid_mask])
            mean_lon = np.mean(lons[valid_mask])
            
            # Swap detection (Heuristic: Europe context)
            if mean_lat < 30 and mean_lon > 30:
                print("DEBUG: Detected swapped coordinates. Swapping back.")
                lats, lons = lons, lats
                
            # Radian detection
            max_val = np.max(np.abs(lats[valid_mask]))
            if max_val < 1.6:
                print("DEBUG: Detected radians. Converting to degrees.")
                lats = np.degrees(lats)
                lons = np.degrees(lons)
                
        # 3. Create Plotly Figure
        print("DEBUG: Converting coordinates to list for JSON serialization")
        fig = go.Figure()
        
        # Prepare hover text
        # Exclude coord columns from hover
        hover_cols = [c for c in current_df.columns if c not in [lat_col, lon_col, time_col]]
        hover_texts = []
        for _, row in current_df.iterrows():
            lines = [f"{col}: {row[col]:.2f}" if isinstance(row[col], (int, float)) else f"{col}: {row[col]}" 
                     for col in hover_cols]
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
        print(f"ERROR: Error generating map figure: {e}")
        raise
