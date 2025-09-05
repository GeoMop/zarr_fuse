import React, { useState, useEffect } from 'react';
import { X, AlertCircle, CheckCircle, Clock, RefreshCw } from 'lucide-react';

interface LogEntry {
  id: string;
  timestamp: string;
  level: 'info' | 'warning' | 'error';
  category: 'backend' | 'store-data';
  message: string;
  resolved?: boolean;
}

interface LogPanelProps {
  show: boolean;
  onClose: () => void;
}

const LogPanel: React.FC<LogPanelProps> = ({ show, onClose }) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState<'all' | 'backend' | 'store-data'>('all');

  // Mock data for now - will be replaced with API call
  const mockLogs: LogEntry[] = [
    {
      id: '1',
      timestamp: new Date().toISOString(),
      level: 'info',
      category: 'backend',
      message: 'S3 connection established successfully',
      resolved: true
    },
    {
      id: '2',
      timestamp: new Date(Date.now() - 60000).toISOString(),
      level: 'warning',
      category: 'store-data',
      message: 'Variable "temperature" contains NaN values in array slice [100:200]',
      resolved: false
    },
    {
      id: '3',
      timestamp: new Date(Date.now() - 120000).toISOString(),
      level: 'error',
      category: 'backend',
      message: 'Failed to connect to S3 endpoint - retrying...',
      resolved: true
    }
  ];

  useEffect(() => {
    if (show) {
      setLoading(true);
      // Simulate API call
      setTimeout(() => {
        setLogs(mockLogs);
        setLoading(false);
      }, 500);
    }
  }, [show]);

  const filteredLogs = logs.filter(log => 
    filter === 'all' || log.category === filter
  );

  const handleResolve = (logId: string) => {
    setLogs(prev => prev.map(log => 
      log.id === logId ? { ...log, resolved: true } : log
    ));
  };

  const getLevelIcon = (level: string) => {
    switch (level) {
      case 'error':
        return <AlertCircle className="w-4 h-4 text-red-500" />;
      case 'warning':
        return <AlertCircle className="w-4 h-4 text-yellow-500" />;
      default:
        return <CheckCircle className="w-4 h-4 text-green-500" />;
    }
  };

  const getLevelColor = (level: string) => {
    switch (level) {
      case 'error':
        return 'border-red-200 bg-red-50';
      case 'warning':
        return 'border-yellow-200 bg-yellow-50';
      default:
        return 'border-green-200 bg-green-50';
    }
  };

  if (!show) return null;

  return (
    <div className="fixed inset-0 z-50 flex">
      {/* Overlay */}
      <div 
        className="flex-1 bg-black bg-opacity-50" 
        onClick={onClose}
      />
      
      {/* Log Panel */}
      <div className="w-1/2 bg-white shadow-2xl flex flex-col">
        {/* Header */}
        <div className="bg-gradient-to-r from-blue-600 to-indigo-700 text-white p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-white/20 rounded-lg">
                <Clock className="w-5 h-5" />
              </div>
              <div>
                <h2 className="font-bold text-xl">Store Logs</h2>
                <p className="text-blue-100 text-sm">System and data monitoring</p>
              </div>
            </div>
            <button
              onClick={onClose}
              className="p-2 bg-blue-100 hover:bg-blue-200 text-blue-600 rounded-lg transition-colors"
              aria-label="Close log panel"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Filter Tabs */}
          <div className="flex gap-2 mt-4">
            {['all', 'backend', 'store-data'].map((filterType) => (
              <button
                key={filterType}
                onClick={() => setFilter(filterType as any)}
                className={`px-3 py-1 rounded text-sm transition-colors ${
                  filter === filterType
                    ? 'bg-white text-blue-600 font-medium'
                    : 'bg-blue-500/30 text-blue-100 hover:bg-blue-500/50'
                }`}
              >
                {filterType === 'all' ? 'All Logs' : 
                 filterType === 'backend' ? 'Backend' : 'Store Data'}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div className="text-center">
                <RefreshCw className="w-8 h-8 text-blue-600 animate-spin mx-auto mb-4" />
                <p className="text-gray-600">Loading logs...</p>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              {filteredLogs.length === 0 ? (
                <div className="text-center py-12">
                  <Clock className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-500">No logs found for selected filter</p>
                </div>
              ) : (
                filteredLogs.map((log) => (
                  <div
                    key={log.id}
                    className={`border rounded-lg p-4 ${getLevelColor(log.level)} ${
                      log.resolved ? 'opacity-75' : ''
                    }`}
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex items-start gap-3 flex-1">
                        {getLevelIcon(log.level)}
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-xs font-medium text-gray-500 uppercase">
                              {log.category}
                            </span>
                            <span className="text-xs text-gray-400">
                              {new Date(log.timestamp).toLocaleString()}
                            </span>
                            {log.resolved && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                RESOLVED
                              </span>
                            )}
                          </div>
                          <p className="text-sm text-gray-700">{log.message}</p>
                        </div>
                      </div>
                      
                      {!log.resolved && log.category === 'store-data' && (
                        <button
                          onClick={() => handleResolve(log.id)}
                          className="ml-3 px-3 py-1 bg-green-600 hover:bg-green-700 text-white text-xs rounded transition-colors"
                        >
                          Mark Resolved
                        </button>
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>
          )}
        </div>

        {/* Footer Stats */}
        <div className="border-t bg-gray-50 p-4">
          <div className="flex justify-between text-sm text-gray-600">
            <span>Total: {logs.length} logs</span>
            <span>Unresolved: {logs.filter(l => !l.resolved && l.category === 'store-data').length}</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LogPanel;
