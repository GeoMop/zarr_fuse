export interface TreeNode {
  id: string;
  name: string;
  type: 'folder' | 'file';
  children?: TreeNode[];
}

export interface SidebarProps {
  isCollapsed: boolean;
  onToggle: () => void;
  onClose: () => void;
}

export interface TreeViewProps {
  nodes: TreeNode[];
  level?: number;
}
