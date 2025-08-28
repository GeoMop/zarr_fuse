import React, { useState } from 'react';

const TreeView: React.FC = () => {
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>({
    test: true,
    weather: true,
  });

  const toggle = (key: 'test' | 'weather') =>
    setOpenGroups((s) => ({ ...s, [key]: !s[key] }));

  return (
    <nav className="zf-tree" aria-label="Data Sources" role="tree">
      <div className="zf-tree__root">Data Sources</div>

      <div className="zf-tree__group" role="treeitem" aria-expanded={openGroups.test}>
        <button className="zf-tree__group-btn" onClick={() => toggle('test')} aria-controls="tree-test">
          <span className="zf-caret" aria-hidden>{openGroups.test ? '▼' : '▶'}</span>
          <span className="zf-tree__label">Test Data</span>
          <span className="zf-badge">2</span>
        </button>
        {openGroups.test && (
          <ul id="tree-test" className="zf-tree__list" role="group">
            <li className="zf-tree__item" role="treeitem">Simple Tree (3)</li>
            <li className="zf-tree__item" role="treeitem">Weather Structure</li>
          </ul>
        )}
      </div>

      <div className="zf-tree__group" role="treeitem" aria-expanded={openGroups.weather}>
        <button className="zf-tree__group-btn" onClick={() => toggle('weather')} aria-controls="tree-weather">
          <span className="zf-caret" aria-hidden>{openGroups.weather ? '▼' : '▶'}</span>
          <span className="zf-tree__label">Weather Data</span>
          <span className="zf-badge">1</span>
        </button>
        {openGroups.weather && (
          <ul id="tree-weather" className="zf-tree__list" role="group">
            <li className="zf-tree__item" role="treeitem">yr.no (11 vars)</li>
          </ul>
        )}
      </div>
    </nav>
  );
};

export default TreeView;


