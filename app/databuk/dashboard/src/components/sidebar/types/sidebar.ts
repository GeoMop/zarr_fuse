export interface ConfigData {
  status: string;
  endpoint: {
    reload_interval: number;
    schema_file: string;
    store_url: string;
    store_type: string;
    version: string;
    description: string;
    // S3 config (optional, if present in backend)
    s3_endpoint_url?: string;
    s3_access_key?: string;
    s3_secret_key?: string;
    s3_region?: string;
    s3_use_ssl?: boolean;
    s3_verify_ssl?: boolean;
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

