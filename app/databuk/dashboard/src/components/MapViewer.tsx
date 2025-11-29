import React, { useEffect, useState } from 'react';
import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-dist-min';
import { API_BASE_URL } from '../api';

Plotly.setPlotConfig({
  mapboxAccessToken: ''
});

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
  const [plotKey, setPlotKey] = useState(0);

  // Force resize when figure loads to fix blank map issues
  useEffect(() => {
    if (!loading && figure) {
        // Small delay to ensure DOM is ready
        const timer = setTimeout(() => {
            window.dispatchEvent(new Event('resize'));
            console.log('Triggered resize event for Plotly');
        }, 100);
        return () => clearTimeout(timer);
    }
  }, [loading, figure]);

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
        console.log('🔍 Backend Raw Response:', data);

        if (!response.ok || data.status === 'error') {
          throw new Error(data.reason || `HTTP error! status: ${response.status}`);
        }

        // 1️⃣ Figure adayını belirle
        let validFigure: any = null;

        // Case 1: Response direkt { data, layout }
        if (Array.isArray(data.data) && data.layout) {
          validFigure = data;
        }
        // Case 2: { status: 'success', figure: { data, layout } }
        else if (data.figure && Array.isArray(data.figure.data) && data.figure.layout) {
          validFigure = data.figure;
        }

        // 2️⃣ Sıkı doğrulama
        if (!validFigure) {
          console.error('Invalid Plotly structure:', data);
          throw new Error("Invalid backend response: JSON must contain 'data' (array) and 'layout' (object).");
        }

        // 3️⃣ Frontend tarafında da template & mapbox normalize et
        if (validFigure.layout) {
          // Template’i tamamen kapat
          validFigure.layout.template = undefined;

          // Mapbox alanını garantile
          validFigure.layout.mapbox = {
            style: 'open-street-map',
            ...(validFigure.layout.mapbox || {}),
          };
        }

        console.log('Figure to render:', validFigure);
        
        // Log layers for debugging
        if (validFigure.layout?.mapbox?.layers) {
            console.log('Map layers detected:', validFigure.layout.mapbox.layers);
        }

        setFigure(validFigure);
        setPlotKey(prev => prev + 1); // Force re-render

      } catch (err) {
        console.error('Failed to fetch map:', err);
        setError(err instanceof Error ? err.message : 'Failed to load map');
      } finally {
        setLoading(false);
      }
    };

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
    <div className="w-full bg-white rounded-lg shadow-sm border border-gray-200 p-4 relative min-h-[500px]">
      <h3 className="text-lg font-semibold text-gray-800 mb-4">Geographic View</h3>
      <Plot
        key={plotKey}
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
