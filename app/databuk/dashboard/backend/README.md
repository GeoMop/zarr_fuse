# ZARR FUSE Dashboard Backend

## Environment Setup (New Approach)

Environment variables are now set via a startup script, not via a `.env` file. The backend reads credentials and configuration directly from the environment.

### Required Environment Variables
- `S3_BUCKET_NAME`: Name of the S3 bucket to use
- `ZF_S3_ACCESS_KEY`: S3 access key for the service user
- `ZF_S3_SECRET_KEY`: S3 secret key for the service user

### How to Start Backend Locally
1. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
2. Use the provided PowerShell script to set environment variables and start the backend:
   ```powershell
   .\start_backend.ps1
   ```
   This will set all required environment variables and launch the backend server.

### Notes
- The backend no longer loads a `.env` file. All configuration must be provided via environment variables.
- For deployment, ensure your environment (e.g., Helm chart, CI/CD pipeline) sets these variables.
- The backend expects these variables to be present at runtime.

## Endpoints Configuration
- The `endpoints.yaml` file now uses `rel_path` instead of a full S3 URL.
- The backend constructs the full S3 URL as:
  ```python
  store_url = f"s3://{os.environ['S3_BUCKET_NAME']}/{endpoints['rel_path']}"
  ```

## Troubleshooting
- If the backend fails to start, check that all required environment variables are set.
- For local development, always use the startup script to avoid missing variables.

---
For further details, see project documentation or contact the maintainers.
