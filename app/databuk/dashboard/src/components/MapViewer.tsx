import React, { useEffect, useState } from 'react';
import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-dist-min';
import { API_BASE_URL } from '../api';

const Plot = createPlotlyComponent(Plotly);

interface MapViewerProps {
  storeName: string;
  nodePath: string;
  selection?: any;
  onMapClick?: (lat: number, lon: number) => void;
}

export const MapViewer: React.FC<MapViewerProps> = ({ 
  storeName, 
  nodePath, 
  selection,
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
            selection: selection,
          }),
        });

        const data = await response.json();

        if (!response.ok || data.status === 'error') {
          throw new Error(data.reason || `HTTP error! status: ${response.status}`);
        }

        // Backend bazen {data:..., layout:...} döner, bazen {figure: {data:..., layout:...}}
        let validFigure: any = null;
        if (Array.isArray(data.data) && data.layout) {
          validFigure = data;
        } else if (data.figure && Array.isArray(data.figure.data) && data.figure.layout) {
          validFigure = data.figure;
        }

        if (!validFigure) {
          throw new Error("Invalid backend response: JSON must contain 'data' and 'layout'.");
        }

        // =========================================================
        // 🛠️ CRITICAL FIX: 'scattermap' -> 'scattermapbox' Conversion
        // =========================================================

        // 1. Force trace types to old (mapbox) format
        validFigure.data.forEach((trace: any) => {
           if (trace.type === 'scattermap') trace.type = 'scattermapbox';
           if (trace.type === 'densitymap') trace.type = 'densitymapbox';
        });

        // 2. If layout.map (new) exists, move to layout.mapbox (old)
        if (validFigure.layout.map) {
            // Copy map object to mapbox
            validFigure.layout.mapbox = { 
                ...validFigure.layout.mapbox, // Keep old settings if any
                ...validFigure.layout.map,    // Overwrite with new settings
                style: 'open-street-map'      // Ensure style is set
            };
            // Prevent conflicts by deleting old key
            delete validFigure.layout.map;
        }

        // 3. Create mapbox object if it doesn't exist
        if (!validFigure.layout.mapbox) {
            validFigure.layout.mapbox = { style: 'open-street-map' };
        }

        // 4. Add default zoom and center settings if missing
        if (!validFigure.layout.mapbox.zoom) validFigure.layout.mapbox.zoom = 5;
        if (!validFigure.layout.mapbox.center) {
            validFigure.layout.mapbox.center = { lat: 50, lon: 14 };
        }

        // =========================================================
        // 🖼️ IMAGE OVERLAY (Raster Layer) Addition
        // =========================================================
        
        if (data.overlay && Array.isArray(data.overlay.corners)) {
          const corners = data.overlay.corners;
          // Use image_url from backend
          const imageUrl = `${API_BASE_URL}${data.overlay.image_url}`;

          const imageLayer = {
            sourcetype: 'image',
            source: imageUrl,
            coordinates: [
              [corners[0][0], corners[0][1]], // Top Left
              [corners[1][0], corners[1][1]], // Top Right
              [corners[2][0], corners[2][1]], // Bottom Right
              [corners[3][0], corners[3][1]]  // Bottom Left
            ],
            opacity: 0, // Fully opaque - no transparency
            below: 'traces', // Image layer stays below data points
          };

          // Initialize or append to existing layers array
          if (!validFigure.layout.mapbox.layers) validFigure.layout.mapbox.layers = [];
          
          // Add image layer to front of array
          validFigure.layout.mapbox.layers = [imageLayer, ...validFigure.layout.mapbox.layers];
        }

        // Remove margins for fullscreen view
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

  }, [storeName, nodePath, selection]);

  // --- RENDER ---

  if (loading) return <div style={{ padding: 20 }}>Loading Map Data...</div>;
  if (error) return <div style={{ padding: 20, color: 'red' }}>Error: {error}</div>;
  if (!figure) return <div style={{ padding: 20 }}>Waiting for data...</div>;

  return (
    <div style={{ width: '100%', height: '600px', border: '1px solid #ddd' }}>
      <Plot
        data={figure.data}
        layout={{
          ...figure.layout,
          width: undefined, // Responsive olması için undefined bırakıyoruz
          height: undefined, // CSS container yüksekliğini alsın
          autosize: true,
        }}
        useResizeHandler={true} // Ekran boyutu değişince haritayı yeniden boyutlandır
        style={{ width: '100%', height: '100%' }}
        config={{ 
            responsive: true,
            scrollZoom: true,
            displayModeBar: true 
        }}
        onClick={(event: any) => {
          // Plotly click event'inden koordinatları al
          if (event.points && event.points[0]) {
            const point = event.points[0];
            const lat = point.lat;
            const lon = point.lon;
            
            if (lat !== undefined && lon !== undefined && onMapClick) {
              console.log('Map clicked at:', { lat, lon });
              onMapClick(lat, lon);
            }
          }
        }}
      />
    </div>
  );
};