import { useState, useEffect } from 'react';

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

interface WeatherStructureResponse {
  variables: WeatherVariable[];
  store_name: string;
  total_variables: number;
  metadata?: Record<string, any>;
}

export const useWeather = () => {
  const [weatherData, setWeatherData] = useState<WeatherVariable[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchWeatherData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch('http://localhost:8000/api/weather/structure');
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data: WeatherStructureResponse = await response.json();
      setWeatherData(data.variables);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch weather data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchWeatherData();
  }, []);

  return {
    weatherData,
    loading,
    error,
    refetch: fetchWeatherData
  };
};
