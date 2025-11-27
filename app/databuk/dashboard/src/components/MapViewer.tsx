import React, { useEffect, useState } from 'react';
import Plot from 'react-plotly.js';
import { API_BASE_URL } from '../api';

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
            selection: selection
          }),
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        
        if (data.status === 'error') {
            throw new Error(data.reason || 'Unknown backend error');
        }

        // Check for nested error in figure
        if (data.figure && data.figure.status === 'error') {
            throw new Error(data.figure.reason || 'Error generating plot');
        }
        
        // If the backend returns the figure directly (as dict) or wrapped
        // My implementation returns the figure dict directly or {status: error}
        // Let's check if it has 'data' and 'layout' keys directly
        if (data.data && data.layout) {
             setFigure(data);
        } else if (data.figure) {
             // In case I wrapped it in previous logic (I didn't in plot_service, but s3_service returns what plot_service returns)
             // Wait, s3_service returns what generate_map_figure returns.
             // generate_map_figure returns fig.to_dict().
             // So it should be the figure object directly.
             setFigure(data.figure);
        } else {
             // Assume data is the figure
             setFigure(data);
        }

      } catch (err) {
        console.error("Failed to fetch map:", err);
        setError(err instanceof Error ? err.message : 'Failed to load map');
      } finally {
        setLoading(false);
      }
    };

    // Cleanup function to revoke object URLs if we were using them (not currently, but good practice)
    fetchMap();
    return () => {
        // cleanup logic if needed
    };
  }, [storeName, nodePath, selection]); // Re-fetch when selection (time) changes

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
    <div className="w-full bg-white rounded-lg shadow-sm border border-gray-200 p-4">
      <h3 className="text-lg font-semibold text-gray-800 mb-4">Geographic View</h3>
      <Plot
        data={figure.data}
        layout={{
            ...figure.layout,
            autosize: true,
            width: undefined, // Let container control width
            height: 500,
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
                // Extract lat/lon from point
                const lat = point.lat;
                const lon = point.lon;
                
                console.log("Map Clicked:", point);
                console.log(`Captured Coordinates -> Lat: ${lat}, Lon: ${lon}`);

                if (lat !== undefined && lon !== undefined && onSelectionChange) {
                    onSelectionChange({ lat_point: lat, lon_point: lon });
                }
            }
        }}
      />
    </div>
  );
};
