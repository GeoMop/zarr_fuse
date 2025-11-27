import numpy as np
import pandas as pd
import polars as pl
import plotly.graph_objects as go
from .store_overview import get_key_for_value

class InteractiveMapPlotter:
    def __init__(self, df, data_selector, handlers):
        """
        Parameters:
        -----------
        df : polars.DataFrame
            Initial DataFrame with columns 'time', 'lon', 'lat', and physical quantities.
        data_selector : dict
            A dictionary with keys 'lon', 'lat', 'time' used to hold the currently selected point.
        handlers : list
            List of objects to notify on update.
        """
        self.handlers = handlers
        self.handlers.append(self)
        self.df = df
        self.data_selector = data_selector
        
        # Determine coordinate keys
        self.time_coord = get_key_for_value(self.data_selector, 'time_axis')
        self.lon_coord = get_key_for_value(self.data_selector, 'lon_axis')
        self.lat_coord = get_key_for_value(self.data_selector, 'lat_axis')

        # Determine physical quantities to visualize
        self.quantities = [col for col in self.df.columns 
                           if col not in [self.time_coord, self.lon_coord, self.lat_coord]]
        self.current_df = None
        
        # Flag to auto-detect if coordinates are swapped (Lat <-> Lon)
        self.is_swapped = False 
        
        # -- PLOTLY FIGURE SETUP --
        self.fig = go.FigureWidget()
        
        # Trace 0: Data Points
        self.fig.add_trace(go.Scattermapbox(
            lat=[], lon=[], mode='markers',
            marker=go.scattermapbox.Marker(size=14, opacity=0.8, color='red', symbol='circle'),
            text=[], hoverinfo='text', name='Data'
        ))
        
        # Map Layout
        self.fig.update_layout(
            mapbox=dict(style="open-street-map", center=dict(lat=0, lon=0), zoom=1),
            margin=dict(l=0, r=0, t=0, b=0), height=600, showlegend=False
        )
        self.fig.data[0].on_click(self.on_click)
        
        print("--- Map Plotter Initialized (Smart Coordinate Detection Enabled) ---")
        self.update_cross()

    def on_click(self, trace, points, selector):
        """Handle map click events."""
        if not points.point_inds: return
        ind = points.point_inds[0]
        row = self.current_df.row(ind, named=True)
        
        # Retrieve raw values
        lat_val = row[self.lat_coord]
        lon_val = row[self.lon_coord]
        
        # If the system detected a swap, we must swap the clicked values too for correct logging
        if self.is_swapped:
            lat_val, lon_val = lon_val, lat_val

        # Radian check (if values are small, convert to degrees for display)
        if abs(lat_val) < 1.6 and abs(lon_val) < 6.3:
            lat_val, lon_val = np.degrees(lat_val), np.degrees(lon_val)

        self.data_selector['lon_point'] = lon_val
        self.data_selector['lat_point'] = lat_val
        
        print(f"Selection: Lat={lat_val:.4f}, Lon={lon_val:.4f}")

        # Highlight selection
        with self.fig.batch_update():
            colors = ['blue'] * len(self.current_df)
            colors[ind] = 'red'
            self.fig.data[0].marker.color = colors
            sizes = [12] * len(self.current_df)
            sizes[ind] = 20
            self.fig.data[0].marker.size = sizes

        # Notify other handlers
        for h in self.handlers:
            if h is not self and hasattr(h, 'update_cross'): h.update_cross()

    def update_cross(self):
        """Update map based on the selected time (finding the nearest match)."""
        selected_time = self.data_selector.get('time_point')
        
        if selected_time is None:
            # Default to the earliest time
            nearest_time = self.df.select(pl.col(self.time_coord)).min().item()
            self.data_selector['time_point'] = nearest_time
        else:
            # Find the nearest time point in the dataset
            df_times = self.df.select(pl.col(self.time_coord).unique())
            times_pd = df_times[self.time_coord].to_pandas()
            
            if not np.issubdtype(times_pd.dtype, np.datetime64): 
                times_pd = pd.to_datetime(times_pd)
            if not isinstance(selected_time, (pd.Timestamp, np.datetime64)): 
                selected_time = pd.to_datetime(selected_time)
            
            nearest_idx = (times_pd - selected_time).abs().idxmin()
            nearest_time = times_pd.iloc[nearest_idx]

        print(f"DEBUG: Selected Time -> {nearest_time}")
        
        self.current_df = self.df.filter(pl.col(self.time_coord) == nearest_time)
        if len(self.current_df) == 0: return

        self._redraw_map()

    def update_data(self, new_df):
        """Update the entire dataset."""
        self.df = new_df
        self.update_cross()

    def _redraw_map(self):
        """Redraw map points, handling coordinate swaps and unit conversions."""
        lats = np.array(self.current_df[self.lat_coord].to_list(), dtype=float)
        lons = np.array(self.current_df[self.lon_coord].to_list(), dtype=float)

        # 1. Clean invalid data (NaN, Inf, Out-of-bounds)
        # World limits: Lat [-90, 90], Lon [-180, 180]. Using 360 as a safe margin.
        valid_mask = np.isfinite(lats) & np.isfinite(lons) & (np.abs(lats) <= 360)
        
        if np.any(valid_mask):
            mean_lat = np.mean(lats[valid_mask])
            mean_lon = np.mean(lons[valid_mask])
            
            # --- SWAP CONTROL (Latitude <-> Longitude) ---
            # Heuristic: If Mean Lat is low (<30) but Mean Lon is high (>30),
            # and assuming European context (e.g., yr.no), coordinates might be swapped.
            # E.g., Arabia (15N, 50E) vs Europe (50N, 15E).
            if mean_lat < 30 and mean_lon > 30:
                print(f">>> DETECTED: Lat ({mean_lat:.1f}) and Lon ({mean_lon:.1f}) appear swapped.")
                print(">>> FIXING: Swapping coordinates...")
                lats, lons = lons, lats
                self.is_swapped = True
            else:
                self.is_swapped = False

            # --- RADIAN CONTROL ---
            # Check again after potential swap. If values are very small, they are likely Radians.
            max_val = np.max(np.abs(lats[valid_mask]))
            if max_val < 1.6:
                print(">>> DETECTED: Coordinates in RADIANS. Converting to DEGREES...")
                lats = np.degrees(lats)
                lons = np.degrees(lons)

        # Prepare hover text
        hover_texts = []
        for row in self.current_df.iter_rows(named=True):
            vals = [f"{q}: {row[q]:.2f}" for q in self.quantities]
            hover_texts.append("<br>".join(vals))

        with self.fig.batch_update():
            self.fig.data[0].lat = lats
            self.fig.data[0].lon = lons
            self.fig.data[0].text = hover_texts
            self.fig.data[0].marker.color = ['blue'] * len(lats)
            self.fig.data[0].marker.size = [12] * len(lats)

            # --- AUTO FOCUS ---
            # Focus only on valid plot data
            plot_mask = np.isfinite(lats) & (np.abs(lats) <= 90)
            if np.any(plot_mask):
                center_lat = np.mean(lats[plot_mask])
                center_lon = np.mean(lons[plot_mask])
                span = max(np.ptp(lats[plot_mask]), np.ptp(lons[plot_mask]))
                
                zoom = 3
                if span < 15: zoom = 5
                if span < 5: zoom = 7
                if span < 1: zoom = 9

                print(f"DEBUG: Focusing -> {center_lat:.2f}, {center_lon:.2f} (Zoom: {zoom})")
                self.fig.layout.mapbox.center = dict(lat=center_lat, lon=center_lon)
                self.fig.layout.mapbox.zoom = zoom