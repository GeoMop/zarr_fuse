import { useState } from 'react';

export const useSidebar = () => {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [isVisible, setIsVisible] = useState(true);

  const toggleCollapse = () => setIsCollapsed(!isCollapsed);
  const hide = () => setIsVisible(false);
  const show = () => setIsVisible(true);

  return {
    isCollapsed,
    isVisible,
    toggleCollapse,
    hide,
    show,
  };
};
