import React from 'react';

const WebsiteInfo: React.FC = () => {
  return (
    <div className="zf-website-info">
      <div className="zf-info-item" title="Website URL">
        <span className="zf-info-icon" aria-hidden>🌐</span>
        <span className="zf-info-text">data-explorer.local</span>
      </div>
      <div className="zf-info-item" title="Version">
        <span className="zf-info-icon" aria-hidden>📅</span>
        <span className="zf-info-text">v1.0.0</span>
      </div>
    </div>
  );
};

export default WebsiteInfo;


