import React from 'react';
import Plot from 'react-plotly.js';

interface TimeSeriesViewerProps {
  data: any;
  onClose?: () => void;
}

export const TimeSeriesViewer: React.FC<TimeSeriesViewerProps> = ({ data, onClose }) => {
  if (!data) return null;

  // Handle nested structure from backend router
  let plotData = data.data;
  let meta = data.meta || {};

  if (!plotData && data.figure) {
      plotData = data.figure.data;
      meta = data.figure.meta || {};
  }

  if (!plotData) {
    return (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 mt-4">
            <h3 className="text-red-800 font-bold">Error: Invalid Data Structure</h3>
            <p className="text-red-600 text-sm">The server response is missing the 'data' field.</p>
            <button onClick={onClose} className="mt-2 text-sm text-red-700 underline">Dismiss</button>
        </div>
    );
  }

  // Find time column
  const keys = Object.keys(plotData);
  const timeKeys = keys.filter(k => 
    k.toLowerCase().includes('time') || k.toLowerCase().includes('date')
  );
  const timeKey = timeKeys.length > 0 ? timeKeys[0] : null;

  if (!timeKey) {
    return (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 mt-4">
            <div className="text-yellow-800 font-bold">Warning: No time dimension found</div>
            <div className="text-sm text-yellow-600">Available keys: {keys.join(', ')}</div>
            <button onClick={onClose} className="mt-2 text-sm text-yellow-700 underline">Dismiss</button>
        </div>
    );
  }

  // Create traces for all other variables (excluding lat/lon/time)
  const traces = Object.keys(plotData)
    .filter(key => 
      key !== timeKey && 
      !key.toLowerCase().includes('lat') && 
      !key.toLowerCase().includes('lon') &&
      !key.toLowerCase().includes('index')
    )
    .map(key => ({
      x: plotData[timeKey],
      y: plotData[key],
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: key,
    }));

  return (
    <div 
        className="w-full bg-white rounded-lg shadow-sm border border-gray-200 flex flex-col mt-6 transition-all duration-300 ease-in-out"
        style={{ 
            height: '500px',
        }}
    >
      <div className="flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200 rounded-t-lg">
        <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-blue-500"></span>
                <h3 className="font-semibold text-gray-700">Time Series Analysis</h3>
            </div>
            {meta.selected_lat && meta.selected_lon && (
                <span className="text-sm text-gray-500 bg-white px-2 py-0.5 rounded border border-gray-200">
                    {meta.selected_lat.toFixed(4)}°N, {meta.selected_lon.toFixed(4)}°E
                </span>
            )}
        </div>
        <button 
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600 hover:bg-gray-100 p-1 rounded transition-colors"
          title="Close Chart"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
      <div className="flex-1 w-full overflow-hidden relative bg-white p-4 rounded-b-lg">
        {traces.length === 0 ? (
             <div className="flex flex-col items-center justify-center h-full text-gray-400">
                <svg className="w-12 h-12 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
                <p>No plottable variables found</p>
             </div>
        ) : (
            <Plot
            data={traces as any}
            layout={{
                autosize: true,
                margin: { l: 50, r: 20, t: 20, b: 40 },
                showlegend: true,
                legend: { orientation: 'h', y: 1.1, x: 0 },
                xaxis: { 
                    title: { text: 'Time' },
                    gridcolor: '#f3f4f6',
                    zerolinecolor: '#e5e7eb'
                },
                yaxis: { 
                    title: { text: 'Value' },
                    gridcolor: '#f3f4f6',
                    zerolinecolor: '#e5e7eb'
                },
                plot_bgcolor: '#ffffff',
                paper_bgcolor: '#ffffff',
                hovermode: 'x unified'
            }}
            config={{
                responsive: true,
                displayModeBar: true,
                displaylogo: false,
                modeBarButtonsToRemove: ['lasso2d', 'select2d']
            }}
            useResizeHandler={true}
            style={{ width: '100%', height: '100%' }}
            />
        )}
      </div>
    </div>
  );
};
