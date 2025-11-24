import { AlertCircle, BarChart3, ChevronDown, ChevronRight, Clock, Database, Folder, RefreshCw, X } from 'lucide-react';
import React, { useCallback, useEffect, useState } from 'react';
import { API_BASE_URL } from '../../api';
import type { SidebarProps } from './types/sidebar';

// Types for S3 data
interface S3Store {
  name: string;
  path: string;
  type: string;
  structure?: any;
  error?: string;
}

interface S3Structure {
  status: string;
  bucket_name: string;
  total_stores: number;
  stores: S3Store[];
}

interface S3Response {
  status: string;
  structure: S3Structure;
}

const Sidebar: React.FC<SidebarProps> = ({
  onClose,
  configData,
  configLoading,
  configError,
  onNodeClick,
  onLogClick
}) => {
  // Endpoint options state (fetched from backend)
  const [endpointOptions, setEndpointOptions] = useState<
    { key: string; label: string; url: string; description: string }[]
  >([]);

  // Selected endpoint state
  const [selectedEndpoint, setSelectedEndpoint] = useState<string>('');

  // Find selected endpoint details
  const selectedEndpointObj = endpointOptions.find(e => e.key === selectedEndpoint);

  // Fetch endpoint list from backend on mount
  useEffect(() => {
    async function fetchEndpoints() {
      try {
        const response = await fetch(`${API_BASE_URL}/api/config/endpoints`);
        const data = await response.json();
        if (data.status === 'success' && data.endpoints) {
          const options = Object.entries(data.endpoints).map(([key, value]) => {
            const endpoint = value as { description?: string; store_url: string };
            return {
              key,
              label: endpoint.description || key,
              url: endpoint.store_url,
              description: endpoint.description ?? "",
            };
          });
          setEndpointOptions(options);
          if (options.length > 0) setSelectedEndpoint(options[0].key);
        }
      } catch (err) {
        // Optionally handle error
      }
    }
    fetchEndpoints();
  }, []);
  // S3 data state
  const [s3Data, setS3Data] = useState<S3Response | null>(null);
  const [s3Loading, setS3Loading] = useState(false);
  const [s3Error, setS3Error] = useState<string | null>(null);

  // Reload/progress state
  const [lastFetchAt, setLastFetchAt] = useState<number | null>(null);
  const [progress, setProgress] = useState<number>(0);

  // Tree expand/collapse state (paths of expanded groups)
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set(['root']));

  // Fetch config and S3 data when selectedEndpoint changes
  useEffect(() => {
    if (!selectedEndpointObj) return;
    const fetchS3Data = async () => {
      setS3Loading(true);
      setS3Error(null);
      try {
        // Example: fetch S3 structure for selected endpoint
        const response = await fetch(`${API_BASE_URL}/api/s3/structure?endpoint=${encodeURIComponent(selectedEndpointObj.key)}`);
        const data = await response.json();
        setS3Data(data);
        setLastFetchAt(Date.now());
      } catch (err) {
        setS3Error('Failed to fetch S3 data');
        setS3Data(null);
      } finally {
        setS3Loading(false);
      }
    };
    fetchS3Data();
  }, [selectedEndpointObj, API_BASE_URL]);
  const toggleExpand = useCallback((path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path); else next.add(path);
      return next;
    });
  }, []);

  const reloadIntervalSec = configData?.endpoint?.reload_interval ?? 60;

  // Fetch S3 data (refactored to component scope)
  const fetchS3Data = useCallback(async () => {
    setS3Loading(true);
    setS3Error(null);
    try {
      // Always use selectedEndpoint for fetch
      if (!selectedEndpointObj) throw new Error('No endpoint selected');
      const response = await fetch(`${API_BASE_URL}/api/s3/structure?endpoint=${encodeURIComponent(selectedEndpointObj.key)}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data: S3Response = await response.json();
      setS3Data(data);
    } catch (error) {
      setS3Error(error instanceof Error ? error.message : 'Failed to fetch S3 data');
    } finally {
      setS3Loading(false);
      setLastFetchAt(Date.now());
      setProgress(0);
    }
  }, [selectedEndpointObj, API_BASE_URL]);

  // Initial fetch on mount
  useEffect(() => {
    fetchS3Data();
  }, [fetchS3Data]);

  // Progress timer tied to Reload_interval; auto-refetch when completes
  useEffect(() => {
    if (!lastFetchAt) return;
    let cancelled = false;
    const tickMs = 1000;
    const totalMs = Math.max(1, reloadIntervalSec) * 1000;

    const intervalId = setInterval(() => {
      if (cancelled) return;
      const elapsed = Date.now() - lastFetchAt;
      const pct = Math.min(100, (elapsed / totalMs) * 100);
      setProgress(pct);
      if (elapsed >= totalMs) {
        // Auto refetch and reset timer
        fetchS3Data();
      }
    }, tickMs);

    return () => {
      cancelled = true;
      clearInterval(intervalId);
    };
  }, [lastFetchAt, reloadIntervalSec, fetchS3Data]);

  // Render tree structure recursively
  const renderTreeItem = (item: any, level: number = 0, parentPath: string = '', storeName: string = '') => {
    const indent = level * 12;
    const itemPath = parentPath ? `${parentPath}/${item.name}` : item.name;

    console.log('Rendering item:', item); // Debug log

    if (item.type === 'group') {
      const groupsOnly = (item.children || []).filter((c: any) => c.type === 'group');
      const isExpanded = expandedPaths.has(itemPath);
      return (
        <div key={itemPath} style={{ marginLeft: indent }}>
          <div
            className="flex items-center gap-2 py-1 hover:bg-gray-100 rounded px-1 cursor-pointer"
            onClick={() => onNodeClick?.(storeName, itemPath)}
          >
            {groupsOnly.length > 0 ? (
              <button
                className="p-0.5 rounded bg-transparent hover:bg-gray-200"
                onClick={(e) => { e.stopPropagation(); toggleExpand(itemPath); }}
                aria-label={isExpanded ? 'Collapse' : 'Expand'}
              >
                {isExpanded ? <ChevronDown className="w-3 h-3 text-gray-600" /> : <ChevronRight className="w-3 h-3 text-gray-600" />}
              </button>
            ) : (
              <span className="inline-block w-3 h-3" />
            )}
            <Folder className="w-4 h-4 text-blue-600" />
            <span className="text-xs font-medium text-gray-700">{item.name}</span>
          </div>
          {isExpanded && groupsOnly.length > 0 && (
            <div className="ml-2">
              {groupsOnly.map((child: any, index: number) => (
                <div key={`${itemPath}/${child.name}-${index}`}>
                  {renderTreeItem(child, level + 1, itemPath, storeName)}
                </div>
              ))}
            </div>
          )}
        </div>
      );
    } else if (item.type === 'array') {
      // Hide arrays (variables) from the tree view
      return null;
    }
    return null;
  };

  // Status colors based on loading/error/content
  const hasStoreError = !!(s3Data?.structure?.stores?.some((s) => s.error));
  const statusColor = s3Loading ? 'bg-yellow-400' : (s3Error || hasStoreError) ? 'bg-red-500' : 'bg-green-400';

  return (
  <aside className="h-screen flex flex-col transition-all duration-300 bg-white border-r border-gray-200 shadow-lg w-full overflow-y-auto scrollbar-thin">
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-600 to-indigo-700 text-white p-3 relative">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="p-1.5 bg-white/20 rounded-lg shadow-lg backdrop-blur-sm">
              <Database className="w-5 h-5 text-white" />
            </div>
            <div>
              <h2 className="font-bold text-lg">ZARR FUSE</h2>
              <p className="text-blue-100 text-xs">Data Platform</p>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={onClose}
              className="p-1.5 bg-blue-100 hover:bg-blue-200 text-blue-600 rounded-lg transition-all duration-200 hover:shadow-md transform hover:scale-105 shadow-sm"
              aria-label="Close sidebar"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Store URL and Description */}
        {configData && (
            <div className="mt-2 pt-2 border-t border-blue-500/30">
              <div className="text-xs">
                <label htmlFor="endpoint-select" className="font-medium mb-1 block">Store Name</label>
                <select
                  id="endpoint-select"
                  value={selectedEndpoint}
                  onChange={e => {
                    setSelectedEndpoint(e.target.value);
                    const params = new URLSearchParams(window.location.search);
                    params.set('store', e.target.value);
                    window.history.replaceState({}, '', `${window.location.pathname}?${params}`);
                  }}
                  className="w-full px-2 py-1 rounded-lg bg-blue-600 text-blue-100 border border-blue-300 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400 focus:border-blue-400 transition-all duration-150 font-semibold hover:bg-blue-700 text-xs"
                  style={{ minHeight: 32 }}
                >
                  {endpointOptions.map(opt => (
                    <option key={opt.key} value={opt.key}>{opt.label}</option>
                  ))}
                </select>
                <div className="font-medium mt-1 text-blue-100 truncate">{selectedEndpointObj?.url}</div>
                <div className="text-blue-100 truncate">{selectedEndpointObj?.description}</div>
              </div>
            </div>
        )}

        {/* Progress Bar - Clickable for reload */}
        {configData && (
          <div className="mt-2 pt-2 border-t border-blue-500/30">
            <div className="flex items-center justify-between text-xs text-blue-100 mb-1">
              <span>Service Status</span>
              <span className="flex items-center gap-1">
                <span className={`inline-block w-1.5 h-1.5 ${statusColor} rounded-full`}></span>
                <span>{s3Loading ? 'Loading' : (s3Error || hasStoreError) ? 'Issues' : 'Active'}</span>
              </span>
            </div>
            <div
              className="w-full bg-blue-500/30 rounded-full h-2 cursor-pointer hover:bg-blue-500/50 transition-colors duration-200"
              onClick={() => {
                fetchS3Data();
              }}
              title="Click to force reload"
            >
              <div className="bg-green-400 h-2 rounded-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
            </div>
            <div className="flex items-center gap-2 mt-1 text-[10px] text-blue-200">
              <Clock className="w-3 h-3" />
              <span>Updated: {new Date(lastFetchAt ?? Date.now()).toLocaleString()}</span>
            </div>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 p-2 overflow-y-auto overflow-x-hidden scrollbar-thin scrollbar-thumb-gray-300 scrollbar-track-gray-100 hover:scrollbar-thumb-gray-400">
        {/* Loading State */}
        {configLoading && (
          <div className="mb-4 p-3 bg-yellow-50 rounded-lg border border-yellow-200 shadow-sm">
            <div className="flex items-center gap-2 mb-2">
              <div className="p-1.5 bg-yellow-500 rounded-lg shadow-md">
                <RefreshCw className="w-4 h-4 text-white animate-spin" />
              </div>
              <div>
                <h3 className="font-semibold text-sm text-yellow-800">Loading Configuration</h3>
                <p className="text-yellow-600 text-xs">Fetching S3 endpoint data...</p>
              </div>
            </div>
          </div>
        )}

        {/* Error State */}
        {configError && (
          <div className="mb-4 p-3 bg-red-50 rounded-lg border border-red-200 shadow-sm">
            <div className="flex items-center gap-2 mb-2">
              <div className="p-1.5 bg-red-500 rounded-lg shadow-md">
                <AlertCircle className="w-4 h-4 text-white" />
              </div>
              <div>
                <h3 className="font-semibold text-sm text-red-800">Configuration Error</h3>
                <p className="text-red-600 text-xs">Failed to load configuration</p>
              </div>
            </div>
            <div className="text-xs text-red-700 mt-1">
              {configError}
            </div>
          </div>
        )}

        {/* Sources */}
        <div className="mb-4 p-3 bg-green-50 rounded-lg border border-green-200 shadow-sm hover:shadow-md transition-shadow duration-200">
          <div className="flex items-center gap-2 mb-2">
            <div className="p-1.5 bg-green-500 rounded-lg shadow-md">
              <BarChart3 className="w-4 h-4 text-white" />
            </div>
            <div>
              <h3 className="font-semibold text-sm text-green-800">Sources</h3>
              <p className="text-green-600 text-xs">Available data sources</p>
            </div>
          </div>

          {/* S3 Loading State */}
          {s3Loading && (
            <div className="flex items-center gap-2 py-1">
              <RefreshCw className="w-3 h-3 text-green-600 animate-spin" />
              <span className="text-xs text-green-600">Loading S3 data...</span>
            </div>
          )}

          {/* S3 Error State */}
          {s3Error && (
            <div className="flex items-center gap-2 py-1">
              <AlertCircle className="w-3 h-3 text-red-600" />
              <span className="text-xs text-red-600">S3 Error: {s3Error}</span>
            </div>
          )}

          {/* S3 Data */}
          {s3Data && s3Data.structure.stores.length > 0 && (
            <div className="space-y-2">
              {s3Data.structure.stores.map((store) => (
                <div key={store.name} className="border border-green-200 rounded-lg p-2 bg-white">
                  <div
                    className="mb-1 cursor-pointer hover:bg-green-50 p-1 rounded transition-colors"
                    onClick={() => onNodeClick?.(store.name, '')}
                  >
                    <span className="font-semibold text-sm text-green-800">{store.name}</span>
                    <span className="text-[10px] text-gray-500 ml-1">(click to view root variables)</span>
                  </div>

                  {store.error ? (
                    <div className="text-xs text-red-600">{store.error}</div>
                  ) : store.structure ? (
                    <div className="space-y-1 pl-2">
                      {(store.structure?.children || [])
                        .filter((c: any) => c.type === 'group')
                        .map((child: any, index: number) => (
                          <div key={`${child.name}-${index}`}>
                            {renderTreeItem(child, 0, '', store.name)}
                          </div>
                        ))}
                    </div>
                  ) : (
                    <div className="text-xs text-gray-500">No structure data</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
      {/* Fixed Footer - Logs */}
      <div className="p-2 border-t border-gray-200 bg-white">
        <div
          className="flex items-center gap-2 cursor-pointer hover:bg-gray-100 p-1.5 rounded-lg transition-colors duration-200"
          onClick={onLogClick}
        >
          <span className={`inline-block w-1.5 h-1.5 ${statusColor} rounded-full`}></span>
          <span className="text-gray-700 text-sm font-medium">Logs</span>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
