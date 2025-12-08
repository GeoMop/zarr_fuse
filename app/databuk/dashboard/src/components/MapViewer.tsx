import React, { useEffect, useState } from 'react';
import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-dist-min';
import { API_BASE_URL } from '../api';

const Plot = createPlotlyComponent(Plotly);

interface MapViewerProps {
  storeName: string;
  nodePath: string;
  selection?: any;
}

export const MapViewer: React.FC<MapViewerProps> = ({ 
  storeName, 
  nodePath, 
  selection
}) => {
  const [figure, setFigure] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!storeName || !nodePath) return;

    const fetchMap = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await fetch(`${API_BASE_URL}/api/s3/plot`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            store_name: storeName,
            node_path: nodePath,
            plot_type: 'map',
            selection: selection,
          }),
        });

        const data = await response.json();

        if (!response.ok || data.status === 'error') {
          throw new Error(data.reason || `HTTP error! status: ${response.status}`);
        }

        // Backend bazen {data:..., layout:...} döner, bazen {figure: {data:..., layout:...}}
        let validFigure: any = null;
        if (Array.isArray(data.data) && data.layout) {
          validFigure = data;
        } else if (data.figure && Array.isArray(data.figure.data) && data.figure.layout) {
          validFigure = data.figure;
        }

        if (!validFigure) {
          throw new Error("Invalid backend response: JSON must contain 'data' and 'layout'.");
        }

        // =========================================================
        // 🛠️ CRITICAL FIX: 'scattermap' -> 'scattermapbox' Dönüşümü
        // =========================================================

        // 1. Trace tiplerini eski (mapbox) formatına zorla
        validFigure.data.forEach((trace: any) => {
           if (trace.type === 'scattermap') trace.type = 'scattermapbox';
           if (trace.type === 'densitymap') trace.type = 'densitymapbox';
        });

        // 2. Eğer layout.map (yeni) varsa, layout.mapbox (eski) içine taşı
        if (validFigure.layout.map) {
            // map objesini mapbox'a kopyala
            validFigure.layout.mapbox = { 
                ...validFigure.layout.mapbox, // Varsa eski ayarları koru
                ...validFigure.layout.map,    // Yeni ayarları üstüne yaz
                style: 'open-street-map'      // Stili garantiye al
            };
            // Çakışmayı önlemek için eski key'i sil
            delete validFigure.layout.map;
        }

        // 3. Mapbox objesi hiç yoksa oluştur
        if (!validFigure.layout.mapbox) {
            validFigure.layout.mapbox = { style: 'open-street-map' };
        }

        // 4. Varsayılan zoom ve center ayarları yoksa ekle
        if (!validFigure.layout.mapbox.zoom) validFigure.layout.mapbox.zoom = 5;
        if (!validFigure.layout.mapbox.center) {
            validFigure.layout.mapbox.center = { lat: 50, lon: 14 };
        }

        // =========================================================
        // 🖼️ IMAGE OVERLAY (Resim Katmanı) Ekleme
        // =========================================================
        
        if (data.overlay && Array.isArray(data.overlay.corners)) {
          const corners = data.overlay.corners;
          // Backend URL'ini kullanarak tam adresi oluştur
          const imageUrl = `${API_BASE_URL}/api/image/mapa_uhelna_vyrez.png`;

          const imageLayer = {
            sourcetype: 'image',
            source: imageUrl,
            coordinates: [
              [corners[0][0], corners[0][1]], // Top Left
              [corners[1][0], corners[1][1]], // Top Right
              [corners[2][0], corners[2][1]], // Bottom Right
              [corners[3][0], corners[3][1]]  // Bottom Left
            ],
            opacity: 0.7, // Altındaki haritayı görmek için hafif şeffaflık
            below: 'traces', // Noktaların altında kalsın
          };

          // Layers array'ini başlat veya mevcut olana ekle
          if (!validFigure.layout.mapbox.layers) validFigure.layout.mapbox.layers = [];
          
          // Resim katmanını en başa ekle
          validFigure.layout.mapbox.layers = [imageLayer, ...validFigure.layout.mapbox.layers];
        }

        // Kenar boşluklarını sıfırla (Tam ekran görünüm için)
        validFigure.layout.margin = { l: 0, r: 0, t: 0, b: 0 };
        validFigure.layout.autosize = true;

        setFigure(validFigure);

      } catch (err) {
        console.error('Failed to fetch map:', err);
        setError(err instanceof Error ? err.message : 'Failed to load map');
      } finally {
        setLoading(false);
      }
    };

    fetchMap();

  }, [storeName, nodePath, selection]);

  // --- RENDER ---

  if (loading) return <div style={{ padding: 20 }}>Loading Map Data...</div>;
  if (error) return <div style={{ padding: 20, color: 'red' }}>Error: {error}</div>;
  if (!figure) return <div style={{ padding: 20 }}>Waiting for data...</div>;

  return (
    <div style={{ width: '100%', height: '600px', border: '1px solid #ddd' }}>
      <Plot
        data={figure.data}
        layout={{
          ...figure.layout,
          width: undefined, // Responsive olması için undefined bırakıyoruz
          height: undefined, // CSS container yüksekliğini alsın
          autosize: true,
        }}
        useResizeHandler={true} // Ekran boyutu değişince haritayı yeniden boyutlandır
        style={{ width: '100%', height: '100%' }}
        config={{ 
            responsive: true,
            scrollZoom: true,
            displayModeBar: true 
        }}
      />
    </div>
  );
};