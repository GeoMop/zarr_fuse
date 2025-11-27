import numpy as np
import pandas as pd
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from .store_overview import get_key_for_value

class MultiZoomer:
    def __init__(self, df, data_selector, handlers):
        """
        Parameters:
        -----------
        df : polars.DataFrame
            The full dataset.
        data_selector : dict
            Shared state dictionary (time, lat, lon).
        handlers : list
            List of objects to notify on update.
        """
        self.handlers = handlers
        self.handlers.append(self)
        self.data_selector = data_selector
        self.df_full = df

        # Identify coordinate keys
        self.time_coord = get_key_for_value(self.data_selector, 'time_axis')
        self.lon_coord = get_key_for_value(self.data_selector, 'lon_axis')
        self.lat_coord = get_key_for_value(self.data_selector, 'lat_axis')

        # Exclude coordinates and metadata columns
        exclude_cols = [self.time_coord, self.lon_coord, self.lat_coord, 'lat_lon', 'grid_domain']
        self.quantities = [col for col in self.df_full.columns if col not in exclude_cols]

        # --- VISUAL CONFIGURATION ---
        self.style_config = {
            'air_temperature': {'color': '#d62728', 'title': 'Air Temp (°C)', 'fill': 'none'},
            'precipitation_amount': {'color': '#1f77b4', 'title': 'Precipitation (mm)', 'fill': 'tozeroy'},
            'relative_humidity': {'color': '#2ca02c', 'title': 'Humidity (%)', 'fill': 'none'},
            'wind_speed': {'color': '#9467bd', 'title': 'Wind Speed (m/s)', 'fill': 'tozeroy'},
            'default': {'color': '#7f7f7f', 'title': 'Value', 'fill': 'none'}
        }

        # -- PLOTLY FIGURE SETUP --
        self.fig = go.FigureWidget(make_subplots(
            rows=len(self.quantities), 
            cols=3, 
            shared_yaxes='rows', 
            column_titles=["<b>Year Overview</b>", "<b>Month View</b>", "<b>Day Detail</b>"],
            vertical_spacing=0.1,
            horizontal_spacing=0.02
        ))

        # Initialize traces
        for i, q in enumerate(self.quantities):
            style = self.style_config.get(q, self.style_config['default'])
            self.fig.update_yaxes(title_text=style['title'], title_font=dict(size=11), row=i+1, col=1)
            
            for j in range(3):
                # Year: Line only, Day: Line + Markers
                if j == 0: mode, width, m_size = 'lines', 1.5, 0
                elif j == 1: mode, width, m_size = 'lines', 2, 0
                else: mode, width, m_size = 'lines+markers', 2.5, 6

                trace = go.Scatter(
                    x=[], y=[], mode=mode, name=style['title'],
                    line=dict(color=style['color'], width=width),
                    marker=dict(size=m_size, color=style['color']),
                    fill=style['fill'] if j == 2 else 'none',
                    showlegend=(j==0), legendgroup=q
                )
                self.fig.add_trace(trace, row=i+1, col=j+1)
        
        # --- LAYOUT SETTINGS (WIDE VIEW) ---
        # Reduced row height (220px) for a wider aspect ratio
        row_height = 220 
        total_height = max(400, row_height * len(self.quantities))

        self.fig.update_layout(
            height=total_height,
            margin=dict(l=50, r=10, t=40, b=10), # Minimal margins
            hovermode="x unified",
            template="plotly_white",
            font=dict(family="Arial", size=11),
            autosize=True,
            title_text="<b>Meteorological Data Analysis</b>",
            title_x=0.5
        )

        # Connect events
        for trace in self.fig.data: trace.on_click(self._on_click)

        print("--- DEBUG: Time Series Initialized (Wide View) ---")
        self.update_cross()

    def _on_click(self, trace, points, selector):
        """Handle click events on graphs."""
        if not points.point_inds: return
        idx = points.point_inds[0]
        clicked_time = trace.x[idx]
        self.data_selector['time_point'] = clicked_time
        print(f"DEBUG: Time selected: {clicked_time}")
        self.update_cross() 
        for h in self.handlers:
            if h is not self and hasattr(h, 'update_cross'): h.update_cross()

    def _add_time_marker(self):
        """Draws the vertical time indicator line."""
        time_point = self.data_selector.get('time_point')
        if time_point is None: return
        
        shapes = []
        total_subplots = len(self.quantities) * 3
        for k in range(1, total_subplots + 1):
            xref = f'x{k}' if k > 1 else 'x'
            shapes.append(dict(
                type="line", xref=xref, yref='paper',
                x0=time_point, x1=time_point, y0=0, y1=1,
                line=dict(color="black", width=1.5, dash="dash"),
                opacity=0.7
            ))
        self.fig.update_layout(shapes=shapes)

    def _get_filtered_data(self):
        """Smart data filtering with Radian/Degree detection."""
        target_lon = self.data_selector.get('lon_point')
        target_lat = self.data_selector.get('lat_point')

        # Default to first row if nothing selected
        if target_lon is None or target_lat is None:
            first_row = self.df_full.row(0, named=True)
            target_lon, target_lat = first_row[self.lon_coord], first_row[self.lat_coord]
            self.data_selector['lon_point'], self.data_selector['lat_point'] = target_lon, target_lat

        # Check data format
        sample_lats = self.df_full.select(pl.col(self.lat_coord).head(100)).to_numpy()
        is_df_radians = np.max(np.abs(sample_lats)) < 1.6
        is_target_degrees = abs(target_lat) > 1.6

        search_lat, search_lon = target_lat, target_lon
        if is_df_radians and is_target_degrees:
            search_lat, search_lon = np.radians(target_lat), np.radians(target_lon)
        elif not is_df_radians and not is_target_degrees:
            search_lat, search_lon = np.degrees(target_lat), np.degrees(target_lon)
        
        epsilon = 0.001 
        
        # Try normal match
        df_filter = self.df_full.filter(
            (pl.col(self.lon_coord).is_between(search_lon - epsilon, search_lon + epsilon)) & 
            (pl.col(self.lat_coord).is_between(search_lat - epsilon, search_lat + epsilon))
        )

        # Try swapped match
        if len(df_filter) == 0:
            df_filter = self.df_full.filter(
                (pl.col(self.lon_coord).is_between(search_lat - epsilon, search_lat + epsilon)) & 
                (pl.col(self.lat_coord).is_between(search_lon - epsilon, search_lon + epsilon))
            )
            
        return search_lon, search_lat, df_filter

    def update_cross(self):
        """Update graphs with new data."""
        used_lon, used_lat, df_filtered = self._get_filtered_data()
        if len(df_filtered) == 0: return

        cols = [self.time_coord] + self.quantities
        df_pd = df_filtered.select(cols).to_pandas().sort_values(by=self.time_coord)
        times = pd.to_datetime(df_pd[self.time_coord])

        with self.fig.batch_update():
            trace_idx = 0
            for q in self.quantities:
                y_vals = df_pd[q]
                for _ in range(3): 
                    self.fig.data[trace_idx].x = times
                    self.fig.data[trace_idx].y = y_vals
                    trace_idx += 1
            
            tp = self.data_selector.get('time_point')
            if tp is None: 
                tp = times.iloc[0]
                self.data_selector['time_point'] = tp
            if not isinstance(tp, pd.Timestamp): tp = pd.to_datetime(tp)

            # Define Zoom Ranges
            range_year = [tp - pd.Timedelta(days=180), tp + pd.Timedelta(days=180)]
            range_month = [tp - pd.Timedelta(days=15), tp + pd.Timedelta(days=15)]
            range_day = [tp - pd.Timedelta(days=2), tp + pd.Timedelta(days=2)]

            cnt = 1
            for _ in self.quantities:
                self.fig.layout[f'xaxis{cnt}' if cnt > 1 else 'xaxis'].range = range_year
                cnt += 1
                self.fig.layout[f'xaxis{cnt}' if cnt > 1 else 'xaxis'].range = range_month
                cnt += 1
                self.fig.layout[f'xaxis{cnt}' if cnt > 1 else 'xaxis'].range = range_day
                cnt += 1

        self._add_time_marker()