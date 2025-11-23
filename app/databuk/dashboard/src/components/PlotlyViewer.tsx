import React, { useEffect, useState } from 'react';
import { API_BASE_URL } from '../api';
import Plot from 'react-plotly.js';

interface PlotlyViewerProps {
  storeName: string;
  nodePath: string;
  plotType?: string;
  selection?: any;
}

const PlotlyViewer: React.FC<PlotlyViewerProps> = ({ storeName, nodePath, plotType, selection }) => {
  const [figure, setFigure] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!storeName || !nodePath) return;
    setLoading(true);
    setError(null);
    setFigure(null);
    const fetchBody = { store_name: storeName, node_path: nodePath, plot_type: plotType, selection };
    fetch(`${API_BASE_URL}/api/s3/plot`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(fetchBody),
    })
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data = await res.json();
        if (data.status === 'success' && data.figure) {
          setFigure(data.figure);
        } else {
          setError(data.reason || 'Unknown error.');
        }
      })
      .catch((err) => {
        setError(err.message);
      })
      .finally(() => setLoading(false));
  }, [storeName, nodePath, plotType, selection]);

  if (loading) return <div>Loading plot...</div>;
  if (error) return <div style={{ color: 'red' }}>{error}</div>;
  if (!figure) return null;

  return (
    <Plot
      data={figure.data}
      layout={figure.layout}
      config={figure.config}
      style={{ width: '100%', height: '400px' }}
      useResizeHandler={true}
    />
  );
};

export default PlotlyViewer;
