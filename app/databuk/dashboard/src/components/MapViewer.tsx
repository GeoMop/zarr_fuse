import React, { useEffect, useState } from 'react';
import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-dist-min';
import { API_BASE_URL } from '../api';

const Plot = createPlotlyComponent(Plotly);

interface MapViewerProps {
  storeName: string;
  nodePath: string;
  selection?: any;
  onSelectionChange?: (selection: any) => void;
}

export const MapViewer: React.FC<MapViewerProps> = ({ 
  storeName, 
  nodePath, 
  selection,
  onSelectionChange 
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

        // 1️⃣ Determine valid figure structure
        let validFigure: any = null;

        // Case 1: Direct structure { data, layout }
        if (Array.isArray(data.data) && data.layout) {
          validFigure = data;
        }
        // Case 2: Wrapped structure { status: 'success', figure: { data, layout } }
        else if (data.figure && Array.isArray(data.figure.data) && data.figure.layout) {
          validFigure = data.figure;
        }

        // 2️⃣ Strict validation
        if (!validFigure) {
          throw new Error("Invalid backend response: JSON must contain 'data' (array) and 'layout' (object).");
        }

        // 3️⃣ Add PNG overlay as a raster image layer if overlay info exists
        if (data.overlay && Array.isArray(data.overlay.corners)) {
          const corners = data.overlay.corners;
          
          // Construct the image URL dynamically using the base API URL
          const imageUrl = `${API_BASE_URL}/api/image/mapa_uhelna_vyrez.png`;

          const imageLayer = {
            sourcetype: 'image',
            source: imageUrl,
            // Coordinates format: [[lon, lat], [lon, lat], [lon, lat], [lon, lat]]
            // Order: Top-Left -> Top-Right -> Bottom-Right -> Bottom-Left
            coordinates: [
              [corners[0][0], corners[0][1]], 
              [corners[1][0], corners[1][1]], 
              [corners[2][0], corners[2][1]], 
              [corners[3][0], corners[3][1]]  
            ],
            opacity: 1,
            below: 'traces', // Ensures the image stays behind the data points
          };

          // Initialize layers array if it doesn't exist
          if (!validFigure.layout.map) {
             validFigure.layout.map = {};
          }
          if (!validFigure.layout.map.layers) {
            validFigure.layout.map.layers = [];
          }
          
          // Prepend the image layer so it renders first (at the bottom)
          validFigure.layout.map.layers = [imageLayer, ...validFigure.layout.map.layers];
        }

        // 4️⃣ Normalize template & map configuration
        if (validFigure.layout) {
          validFigure.layout.template = undefined; // Avoid conflicts
          
          // Merge Map settings carefully to avoid overwriting backend data (like center/zoom)
          validFigure.layout.map = {
            style: 'open-street-map',
            ...validFigure.layout.map, // Preserves existing center, zoom, and layers
          };
        }

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

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-50 rounded-lg border border-gray-200">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-2"></div>
          <p className="text-gray-500">Loading map...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64 bg-red-50 rounded-lg border border-red-200">
        <div className="text-center text-red-600">
          <p className="font-medium">Error loading map</p>
          <p className="text-sm mt-1">{error}</p>
        </div>
      </div>
    );
  }

  if (!figure) return null;

  return (
    <div className="w-full bg-white rounded-lg shadow-sm border border-gray-200 p-4 relative min-h-[500px]">
      <h3 className="text-lg font-semibold text-gray-800 mb-4">Geographic View</h3>
      <Plot
        data={figure.data}
        layout={{
          ...figure.layout,
          autosize: true,
          width: undefined, // Let container control width
          height: 500,
          margin: { t: 0, b: 0, l: 0, r: 0 }, // Minimize margins
        }}
        config={{
          responsive: true,
          displayModeBar: true,
        }}
        style={{ width: '100%', height: '500px' }}
        useResizeHandler={true}
        onClick={(data) => {
          if (data.points && data.points.length > 0) {
            const point = data.points[0] as any;
            const { lat, lon } = point;

            if (lat !== undefined && lon !== undefined && onSelectionChange) {
              onSelectionChange({ lat_point: lat, lon_point: lon });
            }
          }
        }}
        onError={(err) => console.error('Plotly render error:', err)}
      />
    </div>
  );
};