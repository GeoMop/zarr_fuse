import React, { useState } from 'react';
import { Sidebar } from './components/sidebar';
import { Database } from 'lucide-react';

function App() {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [isVisible, setIsVisible] = useState(true);

  return (
    <div className="flex h-screen">
      {/* Global opener when sidebar is hidden (same look/position as header icon) */}
      {!isVisible && (
        <button
          onClick={() => setIsVisible(true)}
          className="fixed left-4 top-4 z-[1000] p-2.5 rounded-xl shadow-lg bg-gradient-to-r from-blue-600 to-indigo-700 text-white"
          aria-label="Open sidebar"
          title="Open sidebar"
        >
          <Database className="w-7 h-7 text-white" />
        </button>
      )}

      {isVisible && (
        <Sidebar 
          isCollapsed={isCollapsed}
          onToggle={() => setIsCollapsed(!isCollapsed)}
          onClose={() => setIsVisible(false)}
        />
      )}
      <main className={`flex-1 p-6 transition-all duration-300 bg-gray-50 ${
        isVisible ? (isCollapsed ? 'ml-20' : 'ml-80') : 'ml-0'
      }`}>
        <div className="max-w-4xl mx-auto">
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8">
            <h1 className="text-3xl font-bold text-gray-800 mb-4">Content Area</h1>
            <p className="text-gray-600 text-lg">This is a placeholder. We are focusing on the sidebar in checkpoint 1.</p>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
