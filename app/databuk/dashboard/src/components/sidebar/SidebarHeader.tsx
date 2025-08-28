import React from 'react';

type Props = {
  onMinimize: () => void;
  onHide: () => void;
};

const SidebarHeader: React.FC<Props> = ({ onMinimize, onHide }) => {
  return (
    <div className="zf-sidebar__header">
      <div className="zf-sidebar__brand">
        <span className="zf-logo" aria-hidden>
          ⛁
        </span>
        <div className="zf-brand__text">
          <h1 className="zf-title">ZARR FUSE</h1>
          <p className="zf-subtitle">Data Explorer</p>
        </div>
      </div>
      <div className="zf-sidebar__controls">
        <button className="zf-btn zf-btn--icon" aria-label="Minimize sidebar" onClick={onMinimize}>
          ─
        </button>
        <button className="zf-btn zf-btn--icon" aria-label="Hide sidebar" onClick={onHide}>
          □
        </button>
      </div>
    </div>
  );
};

export default SidebarHeader;


