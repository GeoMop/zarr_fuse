import React, { useState } from 'react';
import { Folder, FolderOpen, FileText, ChevronRight, ChevronDown } from 'lucide-react';
import type { TreeViewProps } from './types/sidebar';

interface TreeViewPropsExtended extends TreeViewProps {
  isCollapsed?: boolean;
}

export const TreeView: React.FC<TreeViewPropsExtended> = ({ nodes, level = 0, isCollapsed = false }) => {
  const [expanded, setExpanded] = useState<Set<string>>(new Set(['1']));

  const toggleNode = (id: string) => {
    const newExpanded = new Set(expanded);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpanded(newExpanded);
  };

  if (isCollapsed) {
    return (
      <div className="space-y-3">
        {nodes.map((node) => (
          <div key={node.id} className="flex justify-center">
            <div className={`p-3 rounded-xl transition-all duration-200 cursor-pointer transform hover:scale-110 hover:shadow-md ${
              node.type === 'folder' 
                ? expanded.has(node.id)
                  ? 'bg-blue-100 text-blue-600 shadow-sm' 
                  : 'bg-gray-100 text-gray-600 hover:bg-blue-100 hover:text-blue-600 hover:shadow-md'
                : 'bg-gray-100 text-gray-500 hover:bg-blue-50 hover:text-blue-500'
            }`}>
              {node.type === 'folder' ? (
                expanded.has(node.id) ? (
                  <FolderOpen className="w-5 h-5" />
                ) : (
                  <Folder className="w-5 h-5" />
                )
              ) : (
                <FileText className="w-5 h-5" />
              )}
            </div>
          </div>
        ))}
      </div>
    );
  }

  const indentPaddingLeftPx = Math.max(0, level) * 16; // 16px per level

  return (
    <div className="space-y-1">
      {nodes.map((node) => (
        <div key={node.id}>
          <div
            className={"group flex items-center gap-3 px-3 py-2.5 rounded-xl cursor-pointer transition-all duration-200 hover:bg-blue-50 hover:text-blue-700 hover:shadow-sm transform hover:scale-[1.02]"}
            style={{ paddingLeft: indentPaddingLeftPx }}
            onClick={() => node.type === 'folder' && toggleNode(node.id)}
          >
            {/* Expand/Collapse arrow for folders; spacer for files to keep alignment */}
            {node.type === 'folder' ? (
              <div className="flex items-center justify-center w-5 h-5">
                {expanded.has(node.id) ? (
                  <ChevronDown className="w-4 h-4 text-blue-600 transition-transform duration-200" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-gray-500 transition-transform duration-200" />
                )}
              </div>
            ) : (
              <div className="w-5 h-5" />
            )}
            
            {/* Icon - made larger and more prominent */}
            <div className={`p-2.5 rounded-xl transition-all duration-200 shadow-sm ${
              node.type === 'folder' 
                ? expanded.has(node.id)
                  ? 'bg-blue-100 text-blue-600 shadow-md' 
                  : 'bg-gray-100 text-gray-600 group-hover:bg-blue-100 group-hover:text-blue-600 group-hover:shadow-md'
                : 'bg-gray-100 text-gray-500 group-hover:bg-blue-50 group-hover:text-blue-500'
            }`}>
              {node.type === 'folder' ? (
                expanded.has(node.id) ? (
                  <FolderOpen className="w-5 h-5" />
                ) : (
                  <Folder className="w-5 h-5" />
                )
              ) : (
                <FileText className="w-5 h-5" />
              )}
            </div>
            
            {/* Node name - made larger and more readable */}
            <span className={`text-base font-medium truncate flex-1 min-w-0 ${
              node.type === 'folder' ? 'text-gray-800' : 'text-gray-700'
            }`}>
              {node.name}
            </span>
            
            {/* Count badge for folders */}
            {node.type === 'folder' && node.children && (
              <span className="ml-auto text-sm text-gray-500 bg-gray-100 px-3 py-1 rounded-full font-medium flex-shrink-0 shadow-sm">
                {node.children.length}
              </span>
            )}
          </div>
          
          {/* Children */}
          {node.type === 'folder' && expanded.has(node.id) && node.children && (
            <TreeView nodes={node.children} level={level + 1} isCollapsed={isCollapsed} />
          )}
        </div>
      ))}
    </div>
  );
};
