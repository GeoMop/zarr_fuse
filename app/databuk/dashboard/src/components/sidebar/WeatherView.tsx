import React, { useState } from 'react';
import { Folder, FolderOpen, FileText, ChevronRight, ChevronDown, Cloud, Thermometer, MapPin, Clock } from 'lucide-react';

interface WeatherVariable {
  name: string;
  type: string;
  shape: number[];
  unit?: string;
  description?: string;
  coordinates?: string[];
  data_type?: string;
  chunk_shape?: number[];
}

interface WeatherNode {
  id: string;
  name: string;
  type: 'folder' | 'file';
  path: string;
  children?: WeatherNode[];
  variable?: WeatherVariable;
}

interface WeatherViewProps {
  variables: WeatherVariable[];
  isCollapsed: boolean;
  onVariableClick?: (variable: WeatherVariable) => void;
}

const getVariableIcon = (name: string) => {
  const lowerName = name.toLowerCase();
  if (lowerName.includes('temp')) return <Thermometer className="w-5 h-5 text-orange-500" />;
  if (lowerName.includes('lat') || lowerName.includes('lon')) return <MapPin className="w-5 h-5 text-blue-500" />;
  if (lowerName.includes('time')) return <Clock className="w-5 h-5 text-purple-500" />;
  return <Cloud className="w-5 h-5 text-gray-500" />;
};

const formatShape = (shape: number[]) => {
  return shape.join(' × ');
};

const createWeatherTree = (variables: WeatherVariable[]): WeatherNode[] => {
  // Create a root "Weather" folder
  const weatherFolder: WeatherNode = {
    id: 'weather_root',
    name: 'Weather',
    type: 'folder',
    path: 'weather',
    children: variables.map((variable, index) => ({
      id: `weather_var_${index}`,
      name: variable.name,
      type: 'file' as const,
      path: `weather/${variable.name}`,
      variable: variable
    }))
  };
  
  return [weatherFolder];
};

const WeatherView: React.FC<WeatherViewProps> = ({ 
  variables, 
  isCollapsed, 
  onVariableClick 
}) => {
  const [expanded, setExpanded] = useState<Set<string>>(new Set(['weather_root']));
  const weatherTree = createWeatherTree(variables);

  const toggleNode = (id: string) => {
    const newExpanded = new Set(expanded);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpanded(newExpanded);
  };

  const handleNodeClick = (node: WeatherNode) => {
    if (node.type === 'folder') {
      toggleNode(node.id);
    } else if (node.type === 'file' && node.variable && onVariableClick) {
      onVariableClick(node.variable);
    }
  };

  const renderNode = (node: WeatherNode, level: number = 0) => {
    const indentPaddingLeftPx = Math.max(0, level) * 16;

    if (isCollapsed) {
      return (
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
              getVariableIcon(node.name)
            )}
          </div>
        </div>
      );
    }

    return (
      <div key={node.id}>
        <div
          className="group flex items-center gap-3 px-3 py-2.5 rounded-xl cursor-pointer transition-all duration-200 hover:bg-blue-50 hover:text-blue-700 hover:shadow-sm transform hover:scale-[1.02]"
          style={{ paddingLeft: indentPaddingLeftPx }}
          onClick={() => handleNodeClick(node)}
        >
          {/* Expand/Collapse arrow for folders; spacer for files */}
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

          {/* Icon */}
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
              getVariableIcon(node.name)
            )}
          </div>

          {/* Node name */}
          <span className={`text-base font-medium truncate flex-1 min-w-0 ${
            node.type === 'folder' ? 'text-gray-800' : 'text-gray-700'
          }`}>
            {node.name}
          </span>

          {/* Additional info for weather variables */}
          {node.type === 'file' && node.variable && (
            <div className="flex items-center gap-2 ml-auto">
              <span className="text-xs bg-gray-100 px-2 py-1 rounded font-mono text-gray-600">
                {formatShape(node.variable.shape)}
              </span>
              {node.variable.unit && (
                <span className="text-xs bg-blue-100 text-blue-700 px-2 py-1 rounded-full">
                  {node.variable.unit}
                </span>
              )}
            </div>
          )}

          {/* Count badge for folders */}
          {node.type === 'folder' && node.children && (
            <span className="ml-auto text-sm text-gray-500 bg-gray-100 px-3 py-1 rounded-full font-medium flex-shrink-0 shadow-sm">
              {node.children.length}
            </span>
          )}
        </div>

        {/* Children */}
        {node.type === 'folder' && expanded.has(node.id) && node.children && (
          <div className="space-y-1">
            {node.children.map((child) => renderNode(child, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="space-y-1">
      {weatherTree.map((node) => renderNode(node))}
    </div>
  );
};

export default WeatherView;
