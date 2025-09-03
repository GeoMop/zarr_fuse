import React, { useState, useRef, useEffect } from 'react';
import { Sidebar } from './components/sidebar';
import type { ConfigData } from './components/sidebar/types/sidebar';

function App() {
  const [isVisible, setIsVisible] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(420);
  const [isResizing, setIsResizing] = useState(false);
  const [configData, setConfigData] = useState<ConfigData | null>(null);
  const [configLoading, setConfigLoading] = useState(true);
  const [configError, setConfigError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<{storeName: string, nodePath: string} | null>(null);
  const [nodeDetails, setNodeDetails] = useState<any>(null);
  const [nodeLoading, setNodeLoading] = useState(false);
  const [nodeError, setNodeError] = useState<string | null>(null);
  const sidebarRef = useRef<HTMLDivElement>(null);

  // Fetch configuration data
  useEffect(() => {
    const fetchConfig = async () => {
      try {
        setConfigLoading(true);
        const response = await fetch('http://localhost:8000/api/config/current');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        setConfigData(data);
        setConfigError(null);
      } catch (error) {
        console.error('Failed to fetch config:', error);
        setConfigError(error instanceof Error ? error.message : 'Failed to fetch configuration');
      } finally {
        setConfigLoading(false);
      }
    };

    fetchConfig();
  }, []);

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsResizing(true);
    e.preventDefault();
  };

  const handleNodeClick = async (storeName: string, nodePath: string) => {
    console.log('Node clicked:', storeName, nodePath);
    setSelectedNode({ storeName, nodePath });
    setNodeLoading(true);
    setNodeError(null);
    
    try {
      const response = await fetch(`http://localhost:8000/api/s3/node/${storeName}/${nodePath}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setNodeDetails(data.node_details);
    } catch (error) {
      console.error('Failed to fetch node details:', error);
      setNodeError(error instanceof Error ? error.message : 'Failed to fetch node details');
    } finally {
      setNodeLoading(false);
    }
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      
      const newWidth = e.clientX;
      if (newWidth >= 200 && newWidth <= 600) {
        setSidebarWidth(newWidth);
      }
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing]);

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      {isVisible && (
        <div 
          ref={sidebarRef}
          className="relative"
          style={{ width: `${sidebarWidth}px` }}
        >
          <Sidebar 
            onClose={() => setIsVisible(false)} 
            configData={configData}
            configLoading={configLoading}
            configError={configError}
            onNodeClick={handleNodeClick}
          />
          
          {/* Resize Handle */}
          <div
            className="absolute top-0 right-0 w-1 h-full bg-gray-300 hover:bg-blue-500 cursor-col-resize z-10"
            onMouseDown={handleMouseDown}
          />
        </div>
      )}

      {/* Content Area - Sidebar'ın yanında */}
      <main className="flex-1 min-w-0 bg-transparent p-6 border-2 border-dashed border-blue-400/60">
        {/* Show Sidebar Button */}
        {!isVisible && (
          <button
            onClick={() => setIsVisible(true)}
            className="fixed top-4 left-4 z-50 p-3 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow-lg transition-all duration-200 hover:shadow-xl"
            aria-label="Open sidebar"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>
        )}
        
        {/* Node Details Content */}
        {selectedNode ? (
          <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
              <div>
                <h1 className="text-3xl font-bold text-gray-800">
                  Node &apos;{selectedNode.nodePath.split('/').pop()}&apos; Overview
                </h1>
                <p className="text-gray-600 text-lg">
                  xarray DataSet • {selectedNode.storeName}
                </p>
              </div>
            </div>

            {/* Loading State */}
            {nodeLoading && (
              <div className="flex items-center justify-center py-12">
                <div className="text-center">
                  <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
                  <p className="text-gray-600">Loading node details...</p>
                </div>
              </div>
            )}

            {/* Error State */}
            {nodeError && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <div className="flex items-center gap-2">
                  <div className="w-5 h-5 bg-red-500 rounded-full flex items-center justify-center">
                    <span className="text-white text-xs">!</span>
                  </div>
                  <span className="text-red-800 font-medium">Error loading node details</span>
                </div>
                <p className="text-red-600 mt-2">{nodeError}</p>
              </div>
            )}

            {/* Node Details */}
            {nodeDetails && !nodeLoading && (
              <div className="space-y-6">
                {/* ATTRS */}
                {Object.keys(nodeDetails.attrs).length > 0 && (
                  <div className="bg-white rounded-lg border border-gray-200 p-6">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">ATTRS</h2>
                    <div className="space-y-2">
                      {Object.entries(nodeDetails.attrs).map(([key, value]) => (
                        <div key={key} className="flex items-center gap-2">
                          <span className="font-medium text-gray-700">{key}:</span>
                          <span className="text-gray-600">{String(value)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* COORDS */}
                {Object.keys(nodeDetails.coords).length > 0 && (
                  <div className="bg-white rounded-lg border border-gray-200 p-6">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">COORDS</h2>
                    <div className="space-y-4">
                      {Object.entries(nodeDetails.coords).map(([name, coord]: [string, any]) => (
                        <div key={name} className="border border-gray-100 rounded-lg p-4">
                          <h3 className="font-medium text-gray-800 mb-2">{name}</h3>
                          {coord.values && (
                            <div className="space-y-2 text-sm">
                              <div className="flex items-center gap-2">
                                <span className="font-medium">values:</span>
                                <span className="text-gray-600">[{coord.values.sample_data?.slice(0, 5).join(', ')}...]</span>
                              </div>
                              <div className="flex items-center gap-4">
                                <span className="font-medium">min:</span>
                                <span className="text-gray-600">{coord.values.min}</span>
                                <span className="font-medium">max:</span>
                                <span className="text-gray-600">{coord.values.max}</span>
                                <span className="font-medium"># values:</span>
                                <span className="text-gray-600">{coord.values.count}</span>
                              </div>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* VARS */}
                {Object.keys(nodeDetails.vars).length > 0 && (
                  <div className="bg-white rounded-lg border border-gray-200 p-6">
                    <h2 className="text-xl font-semibold text-gray-800 mb-4">VARS</h2>
                    <div className="space-y-4">
                      {Object.entries(nodeDetails.vars).map(([name, variable]: [string, any]) => (
                        <div key={name} className="border border-gray-100 rounded-lg p-4">
                          <h3 className="font-medium text-gray-800 mb-2">{name}</h3>
                          <div className="space-y-2 text-sm">
                            <div className="flex items-center gap-2">
                              <span className="font-medium">shape:</span>
                              <span className="text-gray-600">({variable.shape?.join(', ')})</span>
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="font-medium">coords:</span>
                              <span className="text-gray-600">[coordinate names]</span>
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="font-medium">min - max:</span>
                              <span className="text-gray-600">{variable.min} - {variable.max}</span>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        ) : (
          <div>
            <h1 className="text-3xl font-bold text-gray-800 mb-4">Content Area</h1>
            <p className="text-gray-600 text-lg mb-6">Select a node from the sidebar to view its details.</p>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
