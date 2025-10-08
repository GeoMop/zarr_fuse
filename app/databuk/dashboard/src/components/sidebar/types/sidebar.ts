export interface ConfigData {
  status: string;
  endpoint: {
    Reload_interval: number;
    Schema_file: string;
    STORE_URL: string;
    S3_ENDPOINT_URL: string;
    S3_access_key: string;
    S3_secret_key: string;
    S3_region: string;
    S3_use_ssl: boolean;
    S3_verify_ssl: boolean;
    Description: string;
    Store_type: string;
    Version: string;
  };
}

export interface SidebarProps {
  onClose: () => void;
  configData: ConfigData | null;
  configLoading: boolean;
  configError: string | null;
  onNodeClick?: (storeName: string, nodePath: string) => void;
  onLogClick?: () => void;
}

