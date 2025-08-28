import type { TreeNode } from '../types/sidebar';

export const mockTreeData: TreeNode[] = [
  {
    id: '1',
    name: 'Sources',
    type: 'folder',
    children: [
      {
        id: '1-1',
        name: 'Test Data',
        type: 'folder',
        children: [
          { 
            id: '1-1-1', 
            name: 'Simple Tree', 
            type: 'folder',
            children: [
              { id: '1-1-1-1', name: 'Structure Data', type: 'file' },
              { id: '1-1-1-2', name: 'Transport Data', type: 'file' },
              { id: '1-1-1-3', name: 'Tensors Data', type: 'file' }
            ]
          },
          { 
            id: '1-1-2', 
            name: 'Weather Structure', 
            type: 'folder',
            children: [
              { id: '1-1-2-1', name: 'Temperature', type: 'file' },
              { id: '1-1-2-2', name: 'Humidity', type: 'file' },
              { id: '1-1-2-3', name: 'Pressure', type: 'file' }
            ]
          }
        ]
      },
      {
        id: '1-2',
        name: 'Weather Data',
        type: 'folder',
        children: [
          { 
            id: '1-2-1', 
            name: 'yr.no', 
            type: 'folder',
            children: [
              { id: '1-2-1-1', name: 'Temperature', type: 'file' },
              { id: '1-2-1-2', name: 'Longitude', type: 'file' },
              { id: '1-2-1-3', name: 'Latitude', type: 'file' },
              { id: '1-2-1-4', name: 'Wind Speed', type: 'file' },
              { id: '1-2-1-5', name: 'Wind Direction', type: 'file' },
              { id: '1-2-1-6', name: 'Precipitation', type: 'file' },
              { id: '1-2-1-7', name: 'Cloud Cover', type: 'file' },
              { id: '1-2-1-8', name: 'Visibility', type: 'file' },
              { id: '1-2-1-9', name: 'Dew Point', type: 'file' },
              { id: '1-2-1-10', name: 'UV Index', type: 'file' },
              { id: '1-2-1-11', name: 'Air Quality', type: 'file' }
            ]
          },
          { 
            id: '1-2-2', 
            name: 'OpenWeather', 
            type: 'folder',
            children: [
              { id: '1-2-2-1', name: 'Current Weather', type: 'file' },
              { id: '1-2-2-2', name: 'Forecast', type: 'file' },
              { id: '1-2-2-3', name: 'Historical Data', type: 'file' }
            ]
          }
        ]
      }
    ]
  }
];
