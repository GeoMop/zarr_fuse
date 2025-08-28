import React, { useState } from 'react';
import SidebarHeader from './SidebarHeader';
import WebsiteInfo from './WebsiteInfo';
import TreeView from './TreeView';
import './sidebar.css';

const Sidebar: React.FC = () => {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [isHidden, setIsHidden] = useState(false);

  if (isHidden) {
    return null;
  }

  return (
    <aside
      className={`zf-sidebar ${isCollapsed ? 'zf-sidebar--collapsed' : ''}`}
      aria-label="Sidebar navigation"
    >
      <SidebarHeader
        onMinimize={() => setIsCollapsed((v) => !v)}
        onHide={() => setIsHidden(true)}
      />
      <div className="zf-sidebar__section">
        <WebsiteInfo />
      </div>
      <div className="zf-sidebar__divider" />
      <div className="zf-sidebar__section">
        <TreeView />
      </div>
    </aside>
  );
};

export default Sidebar;


