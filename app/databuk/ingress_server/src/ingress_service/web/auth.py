import os
import json
import logging
import secrets

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

load_dotenv()

LOG = logging.getLogger("auth")
security = HTTPBasic()

def _parse_users_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        LOG.error("BASIC_AUTH_USERS_JSON invalid: %s", e)
        return {}

USERS = _parse_users_json(os.getenv("BASIC_AUTH_USERS_JSON"))

def authenticate(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    stored = USERS.get(credentials.username)
    if stored and secrets.compare_digest(stored, credentials.password):
        return credentials.username

    LOG.warning("BASIC_AUTH failed: username=%r", credentials.username)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Basic"},
    )
