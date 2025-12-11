import React, { useEffect, useState } from 'react';
import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-dist-min';
import { API_BASE_URL } from '../api';

const Plot = createPlotlyComponent(Plotly);

interface MapViewerProps {
  storeName: string;
  nodePath: string;
  onMapClick?: (lat: number, lon: number) => void;
}

export const MapViewer: React.FC<MapViewerProps> = ({ 
  storeName, 
  nodePath, 
  onMapClick
}) => {
  const [figure, setFigure] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!storeName || !nodePath) return;

    const fetchMap = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await fetch(`${API_BASE_URL}/api/s3/plot`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            store_name: storeName,
            node_path: nodePath,
            plot_type: 'map',
          }),
        });

        const data = await response.json();

        if (!response.ok || data.status === 'error') {
          throw new Error(data.reason || `HTTP error! status: ${response.status}`);
        }

        // Backend response can have two formats:
        // Format 1: {data: [...], layout: {...}}
        // Format 2: {figure: {data: [...], layout: {...}}}
        let validFigure: any = null;
        if (Array.isArray(data.data) && data.layout) {
          validFigure = data;
        } else if (data.figure && Array.isArray(data.figure.data) && data.figure.layout) {
          validFigure = data.figure;
        }

        if (!validFigure) {
          throw new Error("Invalid backend response: JSON must contain 'data' and 'layout'.");
        }

        // CRITICAL FIX: Convert trace types from new format to old Plotly format
        // Backend sends scattermap (new MapLibre format), Plotly expects scattermapbox (legacy)
        // This conversion ensures compatibility between backend and frontend rendering

        // Convert trace types to legacy Plotly format for compatibility
        validFigure.data.forEach((trace: any) => {
           if (trace.type === 'scattermap') trace.type = 'scattermapbox';
           if (trace.type === 'densitymap') trace.type = 'densitymapbox';
           
           // Make traces clickable so map click events work
           trace.hoverinfo = trace.hoverinfo || 'text';
           if (trace.mode) {
             trace.mode = trace.mode || 'markers';
           }
        });

        // Move layout configuration from new format (layout.map) to legacy format (layout.mapbox)
        // This ensures all map settings (zoom, center, style) are recognized by Plotly
        if (validFigure.layout.map) {
            // Copy map object to mapbox
            validFigure.layout.mapbox = { 
                ...validFigure.layout.mapbox,
                ...validFigure.layout.map,
                style: 'open-street-map'
            };
            // Remove old key to prevent conflicts
            delete validFigure.layout.map;
        }

        // Ensure mapbox layout object exists with default style
        if (!validFigure.layout.mapbox) {
            validFigure.layout.mapbox = { style: 'open-street-map' };
        }

        // Set default zoom and center if not provided by backend
        if (!validFigure.layout.mapbox.zoom) validFigure.layout.mapbox.zoom = 5;
        if (!validFigure.layout.mapbox.center) {
            validFigure.layout.mapbox.center = { lat: 50, lon: 14 };
        }

        // IMAGE OVERLAY LAYER
        // Adds a raster image (satellite/weather overlay) to the map background
        // Image positioning is defined by corner coordinates from backend
        
        if (data.overlay && Array.isArray(data.overlay.corners)) {
          const corners = data.overlay.corners;
          const imageUrl = `${API_BASE_URL}${data.overlay.image_url}`;

          // Construct image layer in Plotly format
          const imageLayer = {
            sourcetype: 'image',
            source: imageUrl,
            coordinates: [
              [corners[0][0], corners[0][1]], // Top Left
              [corners[1][0], corners[1][1]], // Top Right
              [corners[2][0], corners[2][1]], // Bottom Right
              [corners[3][0], corners[3][1]]  // Bottom Left
            ],
            opacity: 1, // Fully opaque - no transparency
            below: 'traces', // Image layer stays below data pointsS

          };

          // Initialize layers array if it doesn't exist
          if (!validFigure.layout.mapbox.layers) validFigure.layout.mapbox.layers = [];
          
          // Add image layer before data point traces
          validFigure.layout.mapbox.layers = [imageLayer, ...validFigure.layout.mapbox.layers];
        }

        // Remove margins for fullscreen map display
        validFigure.layout.margin = { l: 0, r: 0, t: 0, b: 0 };
        validFigure.layout.autosize = true;

        setFigure(validFigure);

      } catch (err) {
        console.error('Failed to fetch map:', err);
        setError(err instanceof Error ? err.message : 'Failed to load map');
      } finally {
        setLoading(false);
      }
    };

    fetchMap();

  }, [storeName, nodePath]);

  // RENDER
  // Display loading state, error messages, or the map component

  if (loading) return <div style={{ padding: 20 }}>Loading Map Data...</div>;
  if (error) return <div style={{ padding: 20, color: 'red' }}>Error: {error}</div>;
  if (!figure) return <div style={{ padding: 20 }}>Waiting for data...</div>;

  return (
    <div style={{ width: '100%', height: '600px', border: '1px solid #ddd' }}>
      <Plot
        data={figure.data}
        layout={{
          ...figure.layout,
          width: undefined,
          height: undefined,
          autosize: true,
          clickmode: 'event'
        }}
        useResizeHandler={true}
        style={{ width: '100%', height: '100%' }}
        config={{ 
            responsive: true,
            scrollZoom: true,
            displayModeBar: true
        }}
        onClick={(event: any) => {
          // Extract coordinates from Plotly click event
          console.log('Plot clicked event:', event);
          
          if (event.points && event.points[0]) {
            const point = event.points[0];
            console.log('Point data:', point);
            
            const lat = point.lat;
            const lon = point.lon;
            
            if (lat !== undefined && lon !== undefined && onMapClick) {
              console.log('Map clicked at:', { lat, lon });
              onMapClick(lat, lon);
            } else {
              console.log('Missing coordinates - lat:', lat, 'lon:', lon);
            }
          } else {
            console.log('No points found in event');
          }
        }}
      />
    </div>
  );
};