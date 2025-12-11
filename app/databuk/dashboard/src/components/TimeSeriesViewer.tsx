import React, { useState, useEffect, useMemo } from 'react';
import Plot from 'react-plotly.js';

interface TimeSeriesViewerProps {
  data: any;
  loading?: boolean;
  onClose?: () => void;
}

export const TimeSeriesViewer: React.FC<TimeSeriesViewerProps> = ({ data, loading = false, onClose }) => {
  const [selectedTime, setSelectedTime] = useState<string | null>(null);
  const [selectedVariables, setSelectedVariables] = useState<string[]>([]);

  // Prepare data early (no early returns yet!)
  let plotData = data?.data;
  let meta = data?.meta || {};

  if (!plotData && data?.figure) {
      plotData = data.figure.data;
      meta = data.figure.meta || {};
  }

  // Find time column (safe null checks)
  let keys: string[] = [];
  let timeKey: string | null = null;
  
  if (plotData && typeof plotData === 'object' && !Array.isArray(plotData)) {
    keys = Object.keys(plotData);
    const timeKeys = keys.filter(k => 
      k.toLowerCase().includes('time') || k.toLowerCase().includes('date')
    );
    timeKey = timeKeys.length > 0 ? timeKeys[0] : null;
  }

  // ===== HOOKS ÇALIŞIR - HER ZAMAN, HER RENDER'DA =====
  
  // Initialize selectedTime with the first time point if not set
  useEffect(() => {
    if (!selectedTime && plotData && timeKey && plotData[timeKey] && plotData[timeKey].length > 0) {
        const times = plotData[timeKey];
        setSelectedTime(times[Math.floor(times.length / 2)]);
    }
  }, [plotData, timeKey, selectedTime]);

  // Get all available variables
  const availableVariables = useMemo(() => {
      if (!plotData || !timeKey) return [];
      return Object.keys(plotData).filter(key => 
        key !== timeKey && 
        !key.toLowerCase().includes('lat') && 
        !key.toLowerCase().includes('lon') &&
        !key.toLowerCase().includes('index')
      );
  }, [plotData, timeKey]);

  // Initialize selected variables (default to rock_temperature)
  useEffect(() => {
    if (availableVariables.length > 0) {
        if (availableVariables.includes('rock_temperature')) {
            setSelectedVariables(['rock_temperature']);
        } else if (availableVariables.includes('rock_temp')) {
            setSelectedVariables(['rock_temp']);
        } else {
            setSelectedVariables([availableVariables[0]]);
        }
    }
  }, [availableVariables]);

  // ===== HOOKS BITTI =====

  // NOW early returns can happen safely
  if (!data) {
    return (
      <div 
        className="w-full bg-white rounded-lg shadow-sm border border-gray-200 flex flex-col mt-6"
        style={{ 
            height: '500px',
        }}
      >
        <div className="flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200 rounded-t-lg">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-gray-300"></span>
              <h3 className="font-semibold text-gray-700">Time Series Analysis (Multi-Scale Zoom)</h3>
            </div>
          </div>
        </div>
        <div className="flex-1 flex items-center justify-center">
          {loading ? (
            <div className="flex flex-col items-center gap-3">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              <p className="text-gray-500">Loading time series data...</p>
            </div>
          ) : (
            <p className="text-gray-500 text-lg">Select a location on the map to view time series data</p>
          )}
        </div>
      </div>
    );
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

  if (!timeKey) {
    return (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 mt-4">
            <div className="text-yellow-800 font-bold">Warning: No time dimension found</div>
            <div className="text-sm text-yellow-600">Available keys: {keys.join(', ')}</div>
            <button onClick={onClose} className="mt-2 text-sm text-yellow-700 underline">Dismiss</button>
        </div>
    );
  }

  const toggleVariable = (variable: string) => {
      setSelectedVariables(prev => {
          if (prev.includes(variable)) {
              return prev.filter(v => v !== variable);
          } else {
              return [...prev, variable];
          }
      });
  };

  // Create traces for all other variables (excluding lat/lon/time)
  const baseTraces = availableVariables
    .filter(key => selectedVariables.includes(key))
    .map(key => ({
      x: plotData[timeKey],
      y: plotData[key],
      type: 'scatter' as const,
      mode: 'lines' as const, // Lines only for cleaner look in multi-view
      name: key,
      hovertemplate: `<b>${key}</b>: %{y:.2f}<extra></extra>`
    }));

  // Helper to calculate ranges
  const getRange = (centerTime: string, days: number) => {
      if (!centerTime) return undefined;
      const date = new Date(centerTime);
      const start = new Date(date);
      start.setDate(date.getDate() - days);
      const end = new Date(date);
      end.setDate(date.getDate() + days);
      return [start.toISOString(), end.toISOString()];
  };

  const handlePlotClick = (event: any) => {
      if (event.points && event.points[0]) {
          const clickedTime = event.points[0].x;
          console.log("Selected Time:", clickedTime);
          setSelectedTime(clickedTime);
      }
  };

  // Common layout settings
  const commonLayout = {
      autosize: true,
      margin: { l: 40, r: 10, t: 30, b: 30 },
      showlegend: false, // Hide legend on individual plots to save space
      yaxis: { 
          gridcolor: '#f3f4f6',
          zerolinecolor: '#e5e7eb',
          showticklabels: false // Hide Y axis labels for cleaner look (except maybe first one)
      },
      plot_bgcolor: '#ffffff',
      paper_bgcolor: '#ffffff',
      hovermode: 'x unified' as const,
      shapes: selectedTime ? [{
          type: 'line' as const,
          xref: 'x' as const,
          yref: 'paper' as const,
          x0: selectedTime,
          x1: selectedTime,
          y0: 0,
          y1: 1,
          line: {
              color: 'black',
              width: 1,
              dash: 'dash' as const
          }
      }] : []
  };

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
                <h3 className="font-semibold text-gray-700">Time Series Analysis (Multi-Scale Zoom)</h3>
            </div>
            {meta.borehole_id && (
                <span className="text-sm text-gray-600 bg-blue-50 px-2 py-0.5 rounded border border-blue-200 font-medium">
                    Borehole: {meta.borehole_id}
                </span>
            )}
            {selectedTime && (
                <span className="text-xs font-mono bg-blue-50 text-blue-700 px-2 py-0.5 rounded border border-blue-100">
                    Focus: {new Date(selectedTime).toLocaleString()}
                </span>
            )}
        </div>
      </div>
      
      {/* Variable Selection Checkboxes - COMMENTED OUT */}
      {/* <div className="px-4 py-2 bg-white border-b border-gray-100 flex flex-wrap gap-x-4 gap-y-2">
        {availableVariables.map(variable => (
            <label key={variable} className="flex items-center gap-2 text-xs cursor-pointer hover:bg-gray-50 px-2 py-1 rounded border border-transparent hover:border-gray-200 transition-colors">
                <input 
                    type="checkbox" 
                    checked={selectedVariables.includes(variable)}
                    onChange={() => toggleVariable(variable)}
                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 w-3 h-3"
                />
                <span className={selectedVariables.includes(variable) ? 'text-gray-900 font-medium' : 'text-gray-500'}>
                    {variable}
                </span>
            </label>
        ))}
      </div> */}

      <div className="flex-1 w-full overflow-hidden relative bg-white p-2 flex gap-2">
        {baseTraces.length === 0 ? (
             <div className="flex flex-col items-center justify-center h-full w-full text-gray-400">
                <p>No plottable variables found</p>
             </div>
        ) : (
            <>
                {/* View 1: Year Overview (+/- 180 days) */}
                <div className="flex-1 border border-gray-100 rounded relative">
                    <div className="absolute top-2 left-2 z-10 bg-white/80 px-2 py-0.5 text-xs font-bold text-gray-600 rounded border border-gray-200">Year View</div>
                    <Plot
                        data={baseTraces as any}
                        layout={{
                            ...commonLayout,
                            title: undefined,
                            xaxis: { 
                                title: { text: 'Year Overview' },
                                range: selectedTime ? getRange(selectedTime, 180) : undefined,
                                gridcolor: '#f3f4f6'
                            },
                            yaxis: { ...commonLayout.yaxis, showticklabels: true, title: { text: 'Value' } } // Show Y labels on first plot
                        }}
                        config={{ responsive: true, displayModeBar: false }}
                        useResizeHandler={true}
                        style={{ width: '100%', height: '100%' }}
                        onClick={handlePlotClick}
                    />
                </div>

                {/* View 2: Month View (+/- 15 days) */}
                <div className="flex-1 border border-gray-100 rounded relative">
                    <div className="absolute top-2 left-2 z-10 bg-white/80 px-2 py-0.5 text-xs font-bold text-gray-600 rounded border border-gray-200">Month View</div>
                    <Plot
                        data={baseTraces as any}
                        layout={{
                            ...commonLayout,
                            xaxis: { 
                                title: { text: 'Month View' },
                                range: selectedTime ? getRange(selectedTime, 15) : undefined,
                                gridcolor: '#f3f4f6'
                            }
                        }}
                        config={{ responsive: true, displayModeBar: false }}
                        useResizeHandler={true}
                        style={{ width: '100%', height: '100%' }}
                        onClick={handlePlotClick}
                    />
                </div>

                {/* View 3: Day View (+/- 2 days) */}
                <div className="flex-1 border border-gray-100 rounded relative">
                    <div className="absolute top-2 left-2 z-10 bg-white/80 px-2 py-0.5 text-xs font-bold text-gray-600 rounded border border-gray-200">Day View</div>
                    <Plot
                        data={baseTraces.map(t => ({...t, mode: 'lines+markers'})) as any} // Add markers for detail view
                        layout={{
                            ...commonLayout,
                            showlegend: true, // Show legend only on the last plot
                            legend: { orientation: 'h', y: 1.1, x: 0 },
                            xaxis: { 
                                title: { text: 'Day View' },
                                range: selectedTime ? getRange(selectedTime, 2) : undefined,
                                gridcolor: '#f3f4f6'
                            }
                        }}
                        config={{ responsive: true, displayModeBar: false }}
                        useResizeHandler={true}
                        style={{ width: '100%', height: '100%' }}
                        onClick={handlePlotClick}
                    />
                </div>
            </>
        )}
      </div>
    </div>
  );
};
