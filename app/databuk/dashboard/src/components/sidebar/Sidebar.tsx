import React from 'react';
import { ChevronLeft, ChevronRight, X, Database, BarChart3, Globe, Loader2, AlertCircle } from 'lucide-react';

import { TreeView } from './TreeView';
import type { SidebarProps } from './types/sidebar';

const Sidebar: React.FC<SidebarProps> = ({ 
  isCollapsed, 
  onToggle, 
  onClose, 
  treeData, 
  loading, 
  error,
  onFileClick
}) => {
  return (
    <aside className={`h-screen flex flex-col transition-all duration-300 bg-white border-r border-gray-200 shadow-lg ${
      isCollapsed ? 'w-20' : 'w-[420px]'
    }`}>
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-600 to-indigo-700 text-white p-4 relative">
        <div className="flex items-center justify-between">
          {!isCollapsed ? (
            <div className="flex items-center gap-3">
              <div className="p-2.5 bg-white/20 rounded-xl shadow-lg backdrop-blur-sm">
                <Database className="w-7 h-7 text-white" />
              </div>
              <div>
                <h2 className="font-bold text-2xl">ZARR FUSE</h2>
                <p className="text-blue-100 text-base">Data Platform</p>
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-center w-full">
              <div className="p-2.5 bg-white/20 rounded-xl shadow-lg backdrop-blur-sm">
                <Database className="w-7 h-7 text-white" />
              </div>
            </div>
          )}
          <div className="flex gap-2">
            {!isCollapsed && (
              <>
                <button
                  onClick={onToggle}
                  className="p-2.5 bg-blue-100 hover:bg-blue-200 text-blue-600 rounded-xl transition-all duration-200 hover:shadow-md transform hover:scale-105 shadow-sm"
                  aria-label="Collapse sidebar"
                >
                  <ChevronLeft className="w-6 h-6" />
                </button>
                <button
                  onClick={onClose}
                  className="p-2.5 bg-blue-100 hover:bg-blue-200 text-blue-600 rounded-xl transition-all duration-200 hover:shadow-md transform hover:scale-105 shadow-sm"
                  aria-label="Close sidebar"
                >
                  <X className="w-6 h-6" />
                </button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 p-4 overflow-y-auto overflow-x-hidden scrollbar-thin scrollbar-thumb-gray-300 scrollbar-track-gray-100 hover:scrollbar-thumb-gray-400">
        {!isCollapsed ? (
          <>
            {/* Storage Info */}
            <div className="mb-6 p-4 bg-blue-50 rounded-xl border border-blue-200 shadow-sm hover:shadow-md transition-shadow duration-200">
              <div className="flex items-center gap-3 mb-3">
                <div className="p-2.5 bg-blue-500 rounded-xl shadow-md">
                  <BarChart3 className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h3 className="font-semibold text-lg text-blue-800">Storage Info</h3>
                  <p className="text-blue-600 text-base">structure_tree.zarr</p>
                </div>
              </div>
              <div className="space-y-2 text-sm text-blue-700">
                <div className="flex items-center gap-2">
                  <span className="text-blue-600">structure_tree.zarr</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="inline-block w-2 h-2 bg-green-500 rounded-full"></span>
                  <span>Active</span>
                </div>
                <div className="flex items-center gap-2">
                  <span>Updated 2 hours ago</span>
                </div>
                <div className="flex items-center gap-2">
                  <span>{treeData.length} nodes available</span>
                </div>
              </div>
            </div>

            {/* Sources Section */}
            <div className="mb-4">
              <h4 className="text-base font-semibold text-gray-800 mb-3 flex items-center gap-2">
                <div className="p-2 bg-gray-100 rounded-xl shadow-sm">
                  <Globe className="w-5 h-5 text-gray-600" />
                </div>
                Sources
              </h4>
            </div>

            {/* Tree View with Loading/Error States */}
            <div className="p-4">
              {loading ? (
                <div className="flex items-center justify-center py-8">
                  <div className="flex items-center gap-3 text-blue-600">
                    <Loader2 className="w-6 h-6 animate-spin" />
                    <span className="text-lg">Loading tree structure...</span>
                  </div>
                </div>
              ) : error ? (
                <div className="flex items-center justify-center py-8">
                  <div className="flex items-center gap-3 text-red-600">
                    <AlertCircle className="w-6 h-6" />
                    <div className="text-center">
                      <p className="font-medium">Failed to load data</p>
                      <p className="text-sm text-red-500">{error}</p>
                    </div>
                  </div>
                </div>
              ) : treeData.length === 0 ? (
                <div className="flex items-center justify-center py-8">
                  <div className="text-center text-gray-500">
                    <Globe className="w-8 h-8 mx-auto mb-2" />
                    <p>No data sources available</p>
                  </div>
                </div>
              ) : (
                <TreeView 
                  nodes={treeData} 
                  isCollapsed={isCollapsed} 
                  onFileClick={onFileClick}
                />
              )}
            </div>
          </>
        ) : (
          /* Collapsed State - Icons only */
          <div className="space-y-4">
            {/* Expand button as first icon in column */}
            <div className="flex justify-center">
              <button
                onClick={onToggle}
                className="p-3 bg-blue-100 rounded-xl shadow-sm hover:shadow-md transition-all duration-200 cursor-pointer transform hover:scale-110 text-blue-600"
                aria-label="Expand sidebar"
              >
                <ChevronRight className="w-6 h-6" />
              </button>
            </div>

            {/* Platform Info Icon */}
            <div className="flex justify-center">
              <div className="p-3 bg-blue-100 rounded-xl shadow-sm hover:shadow-md transition-all duration-200 cursor-pointer transform hover:scale-110">
                <BarChart3 className="w-6 h-6 text-blue-600" />
              </div>
            </div>

            {/* Sources Icon */}
            <div className="flex justify-center">
              <div className="p-3 bg-gray-100 rounded-xl shadow-sm hover:shadow-md transition-all duration-200 cursor-pointer transform hover:scale-110">
                <Globe className="w-6 h-6 text-gray-600" />
              </div>
            </div>

            {/* Tree View Icons - Show loading or data */}
            <div className="p-3">
              {loading ? (
                <div className="flex justify-center">
                  <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
                </div>
              ) : error ? (
                <div className="flex justify-center">
                  <AlertCircle className="w-5 h-5 text-red-600" />
                </div>
              ) : (
                <TreeView 
                  nodes={treeData} 
                  isCollapsed={isCollapsed}
                  onFileClick={onFileClick}
                />
              )}
            </div>
          </div>
        )}
      </div>
    </aside>
  );
};

export default Sidebar;


