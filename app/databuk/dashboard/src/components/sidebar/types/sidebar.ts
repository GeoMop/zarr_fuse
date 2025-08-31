export interface TreeNode {
  id: string;
  name: string;
  type: 'folder' | 'file';
  path: string;
  children?: TreeNode[];
}

export interface SidebarProps {
  isCollapsed: boolean;
  onToggle: () => void;
  onClose: () => void;
  treeData: TreeNode[];
  loading: boolean;
  error: string | null;
  onFileClick?: (filePath: string, fileName: string) => void;
}

export interface TreeViewProps {
  nodes: TreeNode[];
  level?: number;
  isCollapsed?: boolean;
  onFileClick?: (filePath: string, fileName: string) => void;
}
