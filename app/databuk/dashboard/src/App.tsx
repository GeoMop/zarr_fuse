import React, { useState, useEffect } from 'react';
import { Sidebar } from './components/sidebar';
import { Database } from 'lucide-react';

// Types for the tree data from backend
interface TreeNode {
  id: string;
  name: string;
  type: 'folder' | 'file';
  path: string;
  children?: TreeNode[];
}

interface TreeResponse {
  nodes: TreeNode[];
  store_name: string;
  total_nodes: number;
}

interface FileData {
  path: string;
  name: string;
  type: string;
  shape: number[];
  dtype: string;
  size: number;
  data: any[];
  metadata: any;
  sample_size: number;
}

function App() {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [isVisible, setIsVisible] = useState(true);
  const [treeData, setTreeData] = useState<TreeNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<FileData | null>(null);
  const [fileLoading, setFileLoading] = useState(false);

  // Fetch tree data from backend
  useEffect(() => {
    const fetchTreeData = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const response = await fetch('http://localhost:8000/api/tree/structure');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data: TreeResponse = await response.json();
        setTreeData(data.nodes);
        console.log('Tree data loaded:', data);
      } catch (err) {
        console.error('Failed to fetch tree data:', err);
        setError(err instanceof Error ? err.message : 'Failed to fetch data');
      } finally {
        setLoading(false);
      }
    };

    fetchTreeData();
  }, []);

  // Handle file click from sidebar
  const handleFileClick = async (filePath: string, fileName: string) => {
    try {
      setFileLoading(true);
      setSelectedFile(null);
      
      console.log('Fetching file data for:', filePath);
      
      const response = await fetch(`http://localhost:8000/api/tree/file/data?path=${encodeURIComponent(filePath)}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const fileData: FileData = await response.json();
      setSelectedFile(fileData);
      console.log('File data loaded:', fileData);
    } catch (err) {
      console.error('Failed to fetch file data:', err);
      setError(err instanceof Error ? err.message : 'Failed to fetch file data');
    } finally {
      setFileLoading(false);
    }
  };

  // Global opener when sidebar is hidden
  if (!isVisible) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <button
          onClick={() => setIsVisible(true)}
          className="fixed left-4 top-4 z-[1000] p-2.5 rounded-xl shadow-lg bg-gradient-to-r from-blue-600 to-indigo-700 text-white"
          aria-label="Open sidebar"
          title="Open sidebar"
        >
          <Database className="w-7 h-7 text-white" />
        </button>
        <main className="flex-1 p-6 bg-gray-50 ml-0">
          <div className="max-w-4xl mx-auto">
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8">
              <h1 className="text-3xl font-bold text-gray-800 mb-4">ZARR FUSE Dashboard</h1>
              <p className="text-gray-600 text-lg mb-6">
                Welcome to the Data Explorer Platform. This is a placeholder for the main content area.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="p-6 bg-blue-50 rounded-lg border border-blue-200">
                  <h3 className="text-lg font-semibold text-blue-800 mb-2">Project Status</h3>
                  <p className="text-blue-600">Basic setup completed successfully</p>
                </div>
                <div className="p-6 bg-green-50 rounded-lg border border-green-200">
                  <h3 className="text-lg font-semibold text-green-800 mb-2">Next Steps</h3>
                  <p className="text-green-600">Ready for sidebar development</p>
                </div>
              </div>
            </div>
          </div>
        </main>
      </div>
    );
  }

  return (
    <div className="flex h-screen">
      <Sidebar
        isCollapsed={isCollapsed}
        onToggle={() => setIsCollapsed(!isCollapsed)}
        onClose={() => setIsVisible(false)}
        treeData={treeData}
        loading={loading}
        error={error}
        onFileClick={handleFileClick}
      />
      <main className={`flex-1 p-6 transition-all duration-300 bg-gray-50 ${
        isVisible ? (isCollapsed ? 'ml-20' : 'ml-[420px]') : 'ml-0'
      }`}>
        <div className="max-w-4xl mx-auto">
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8">
            <h1 className="text-3xl font-bold text-gray-800 mb-4">Content Area</h1>
            <p className="text-gray-600 text-lg mb-6">This is a placeholder. We are focusing on the sidebar in checkpoint 1.</p>
            
            {/* File Data Display */}
            {selectedFile && (
              <div className="mb-6 p-6 bg-blue-50 rounded-xl border border-blue-200">
                <h2 className="text-2xl font-bold text-blue-800 mb-4">📁 {selectedFile.name}</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                  <div>
                    <h3 className="font-semibold text-blue-700 mb-2">File Information</h3>
                    <div className="space-y-2 text-sm">
                      <div><span className="font-medium">Path:</span> {selectedFile.path}</div>
                      <div><span className="font-medium">Type:</span> {selectedFile.type}</div>
                      <div><span className="font-medium">Shape:</span> {selectedFile.shape.join(' × ')}</div>
                      <div><span className="font-medium">Data Type:</span> {selectedFile.dtype}</div>
                      <div><span className="font-medium">Size:</span> {selectedFile.size.toLocaleString()}</div>
                      <div><span className="font-medium">Sample Size:</span> {selectedFile.sample_size.toLocaleString()}</div>
                    </div>
                  </div>
                  <div>
                    <h3 className="font-semibold text-blue-700 mb-2">Data Preview</h3>
                    <div className="bg-white p-3 rounded-lg border max-h-40 overflow-y-auto">
                      <pre className="text-xs text-gray-700">
                        {JSON.stringify(selectedFile.data, null, 2)}
                      </pre>
                    </div>
                  </div>
                </div>
                {selectedFile.metadata && Object.keys(selectedFile.metadata).length > 0 && (
                  <div>
                    <h3 className="font-semibold text-blue-700 mb-2">Metadata</h3>
                    <div className="bg-white p-3 rounded-lg border">
                      <pre className="text-xs text-gray-700">
                        {JSON.stringify(selectedFile.metadata, null, 2)}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* File Loading State */}
            {fileLoading && (
              <div className="mb-6 p-6 bg-blue-50 rounded-xl border border-blue-200">
                <div className="flex items-center gap-3 text-blue-600">
                  <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
                  <span className="text-lg">Loading file data...</span>
                </div>
              </div>
            )}

            {/* Instructions */}
            {!selectedFile && !fileLoading && (
              <div className="mb-6 p-6 bg-gray-50 rounded-xl border border-gray-200">
                <h3 className="font-semibold text-gray-800 mb-2">💡 How to use</h3>
                <p className="text-gray-600">
                  Click on any file (📄) in the sidebar to view its data and metadata here.
                  Folders (📁) can be expanded/collapsed by clicking on them.
                </p>
              </div>
            )}
            
            {/* Debug info */}
            <div className="mt-6 p-4 bg-gray-50 rounded-lg">
              <h3 className="font-semibold text-gray-800 mb-2">Backend Status:</h3>
              {loading && <p className="text-blue-600">Loading tree data...</p>}
              {error && <p className="text-red-600">Error: {error}</p>}
              {!loading && !error && (
                <p className="text-green-600">
                  Tree data loaded successfully! Total nodes: {treeData.length}
                </p>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
