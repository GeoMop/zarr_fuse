import os
import yaml

from pathlib import Path
from dotenv import load_dotenv
from .models import S3Config, EndpointConfig, ScrapperConfig, UnitedDataSourceConfig

import boto3
from botocore.config import Config
from botocore.client import BaseClient

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]

def load_s3_config() -> S3Config:
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    region = os.getenv("S3_REGION", "us-east-1")
    store_url = os.getenv("S3_STORE_URL")

    if not access_key:
        raise RuntimeError("S3_ACCESS_KEY must be set")
    if not secret_key:
        raise RuntimeError("S3_SECRET_KEY must be set")
    if not endpoint_url:
        raise RuntimeError("S3_ENDPOINT_URL must be set")
    if not store_url:
        raise RuntimeError("S3_STORE_URL must be set")

    return S3Config(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_url=endpoint_url,
        region=region,
        store_url=store_url,
    )


def load_endpoints_config() -> list[EndpointConfig]:
    cfg_path = BASE_DIR / "inputs/endpoints_config.yaml"

    with open(cfg_path, "r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    return [EndpointConfig(**ec) for ec in raw.get("endpoints", [])]

def load_scrappers_config() -> list[ScrapperConfig]:
    cfg_path = BASE_DIR / "inputs/endpoints_config.yaml"

    with open(cfg_path, "r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    return [ScrapperConfig(**sc) for sc in raw.get("active_scrappers", [])]

def create_boto3_client() -> BaseClient:
    cfg = load_s3_config()
    config = Config(
        signature_version="s3v4",
        s3={
            "addressing_style": "path",
        },
        #request_checksum_calculation="WHEN_REQUIRED",
        #response_checksum_validation="WHEN_REQUIRED",
        retries={"max_attempts": 3, "mode": "standard"},
    )
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        endpoint_url=cfg.endpoint_url,
        config=config
    )

def load_united_data_source_config() -> list[UnitedDataSourceConfig]:
    """
    Načte všechno (endpoints + scrappers) a vrátí jako jeden seznam.
    Může to být klidně z jednoho YAML, nebo můžeš interně volat
    load_endpoints_config() + load_scrappers_config().
    """
    """
    """
    endpoints = load_endpoints_config()
    scrappers = load_scrappers_config()
    return [*endpoints, *scrappers]
