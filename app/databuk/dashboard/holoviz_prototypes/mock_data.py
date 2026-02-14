"""
Mock data generation for HoloViz prototype dashboard.
Contains functions to generate sample datasets for demonstration purposes.
"""
import numpy as np
import pandas as pd


def generate_timeseries_data(n_points=100, seed=42):
    """
    Generate mock time-series data with X, Y coordinates and temperature.
    
    Args:
        n_points: Number of data points to generate
        seed: Random seed for reproducibility
        
    Returns:
        pandas.DataFrame with columns: time, x, y, temperature
    """
    np.random.seed(seed)
    
    times = pd.date_range('2024-01-01', periods=n_points, freq='h')
    x_vals = np.cumsum(np.random.randn(n_points)) + 10
    y_vals = np.cumsum(np.random.randn(n_points)) + 20
    temperature = 15 + 5 * np.sin(np.linspace(0, 4*np.pi, n_points)) + np.random.randn(n_points)
    
    df = pd.DataFrame({
        'time': times,
        'x': x_vals,
        'y': y_vals,
        'temperature': temperature
    })
    
    return df


def generate_geographic_data(n_points=30, seed=123):
    """
    Generate mock geographic data points in Central Europe.
    
    Args:
        n_points: Number of geographic points to generate
        seed: Random seed for reproducibility
        
    Returns:
        pandas.DataFrame with columns: lon, lat, value
    """
    np.random.seed(seed)
    
    map_lons = np.random.uniform(10, 20, n_points)  # Longitude range (Central Europe)
    map_lats = np.random.uniform(45, 55, n_points)  # Latitude range
    map_values = np.random.uniform(0, 100, n_points)  # Measurement values
    
    map_df = pd.DataFrame({
        'lon': map_lons,
        'lat': map_lats,
        'value': map_values
    })
    
    return map_df


def get_overlay_bounds():
    """
    Get bounds for the map overlay rectangle.
    
    Returns:
        tuple: (lon_min, lat_min, lon_max, lat_max)
    """
    return (12, 47, 18, 53)
