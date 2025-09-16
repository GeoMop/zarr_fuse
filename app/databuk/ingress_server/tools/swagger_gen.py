#!/usr/bin/env python3
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
cfg_path = ROOT / "src" / "endpoints_config.yaml"
out_path = ROOT / "docs" / "swagger.yaml"

BASE = {
    "openapi": "3.0.3",
    "info": {
        "title": "ZarrFuse Ingress API",
        "version": "0.2.0",
        "description": "Backend-only service to ingest CSV/JSON and merge into an S3-backed Zarr store.",
    },
    "servers": [{"url": "http://localhost:8000"}],
    "paths": {},
}

ROOT_OP = {
    "post": {
        "summary": "Upload data to the root node of the Zarr tree",
        "requestBody": {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["file"],
                        "properties": {
                            "file": {"type": "string", "format": "binary"}
                        },
                    }
                }
            },
        },
        "responses": {
            "200": {"description": "Successful update"},
            "400": {"description": "Validation or processing error"},
        },
    }
}

NODE_OP = {
    "post": {
        "summary": "Upload data to a specific node path in the Zarr tree",
        "parameters": [
            {
                "in": "path",
                "name": "node_path",
                "required": True,
                "schema": {"type": "string"},
                "description": "Zarr node path, e.g. a/b/c",
            }
        ],
        "requestBody": {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["file"],
                        "properties": {
                            "file": {"type": "string", "format": "binary"}
                        },
                    }
                }
            },
        },
        "responses": {
            "200": {"description": "Successful update"},
            "400": {"description": "Validation or processing error"},
        },
    }
}

def main():
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths = {}
    names = []
    for ep in cfg.get("endpoints", []):
        name = ep["name"]
        endpoint = ep["endpoint"].rstrip("/")
        names.append(name)

        paths[f"{endpoint}"] = ROOT_OP

        paths[f"{endpoint}" + "/{node_path}"] = NODE_OP

    BASE["paths"] = paths

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(BASE, f, sort_keys=False)
    print(f"Generated {out_path}")

if __name__ == "__main__":
    main()
