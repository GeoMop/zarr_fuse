import React from 'react';
import Plot from 'react-plotly.js';

interface TimeSeriesViewerProps {
  data: any;
  onClose?: () => void;
}

export const TimeSeriesViewer: React.FC<TimeSeriesViewerProps> = ({ data, onClose }) => {
  console.log("TimeSeriesViewer received data:", data);

  if (!data || !data.data) {
    console.log("No data or data.data found");
    return null;
  }

  const plotData = data.data;
  const meta = data.meta || {};
  
  // Find time column
  const keys = Object.keys(plotData);
  console.log("Available keys:", keys);

  const timeKeys = keys.filter(k => 
    k.toLowerCase().includes('time') || k.toLowerCase().includes('date')
  );
  const timeKey = timeKeys.length > 0 ? timeKeys[0] : null;
  console.log("Detected time key:", timeKey);

  if (!timeKey) {
    return (
        <div className="fixed bottom-0 left-0 right-0 bg-white border-t-2 border-red-500 shadow-lg z-[100] h-40 p-4">
            <div className="text-red-600 font-bold">Error: No time dimension found in data</div>
            <div className="text-sm text-gray-600">Available keys: {keys.join(', ')}</div>
            <button onClick={onClose} className="mt-2 px-4 py-2 bg-gray-200 rounded">Close</button>
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
  
  console.log("Generated traces:", traces.length);

  return (
    <div className="fixed bottom-0 left-0 right-0 bg-white border-t-2 border-blue-500 shadow-lg z-[100] h-80 flex flex-col">
      <div className="flex items-center justify-between p-2 bg-gray-100 border-b">
        <h3 className="font-semibold text-gray-700">
          Time Series Analysis 
          {meta.selected_lat && meta.selected_lon && (
            <span className="text-sm font-normal ml-2 text-gray-500">
              ({meta.selected_lat.toFixed(4)}, {meta.selected_lon.toFixed(4)})
            </span>
          )}
        </h3>
        <button 
          onClick={onClose}
          className="p-1 hover:bg-gray-200 rounded text-gray-600"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
      <div className="flex-1 w-full overflow-hidden relative">
        {traces.length === 0 ? (
             <div className="flex items-center justify-center h-full text-gray-500">No variables to plot</div>
        ) : (
            <Plot
            data={traces as any}
            layout={{
                autosize: true,
                margin: { l: 50, r: 20, t: 20, b: 40 },
                showlegend: true,
                legend: { orientation: 'h', y: 1.1 },
                xaxis: { title: { text: 'Time' } },
                yaxis: { title: { text: 'Value' } }
            }}
            useResizeHandler={true}
            style={{ width: '100%', height: '100%' }}
            />
        )}
      </div>
    </div>
  );
};
