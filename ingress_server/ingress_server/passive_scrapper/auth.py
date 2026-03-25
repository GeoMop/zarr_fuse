import os
import json
import logging
import secrets

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

LOG = logging.getLogger(__name__)
load_dotenv()

SECURITY = HTTPBasic()


def _parse_users_json(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            LOG.error("BASIC_AUTH_USERS_JSON must be a JSON object")
            return {}
        return parsed

    except json.JSONDecodeError as e:
        LOG.error("BASIC_AUTH_USERS_JSON invalid: %s", e)
        return {}


USERS = _parse_users_json(os.getenv("BASIC_AUTH_USERS_JSON"))


def verify_basic_auth(
    credentials: HTTPBasicCredentials = Depends(SECURITY),
) -> str:
    username = credentials.username
    password = credentials.password

    expected_password = USERS.get(username)

    if expected_password and secrets.compare_digest(expected_password, password):
        return username

    LOG.warning("BASIC_AUTH failed: username=%r", username)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Basic"},
    )
