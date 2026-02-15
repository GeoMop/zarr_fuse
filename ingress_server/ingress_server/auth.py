import os
import json
import logging
from flask_httpauth import HTTPBasicAuth
from dotenv import load_dotenv

LOG = logging.getLogger("auth")
load_dotenv()

def _parse_users_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        LOG.error("BASIC_AUTH_USERS_JSON invalid: %s", e)
        return {}

AUTH = HTTPBasicAuth()
USERS = _parse_users_json(os.getenv("BASIC_AUTH_USERS_JSON"))

@AUTH.verify_password
def verify_password(username: str, password: str) -> str | None:
    if username in USERS and USERS[username] == password:
        return username
    else:
        LOG.warning("BASIC_AUTH failed: username=%r", username)
    return None
