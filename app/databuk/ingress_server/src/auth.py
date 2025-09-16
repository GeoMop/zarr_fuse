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
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "true").lower() == "true"

@AUTH.verify_password
def verify_password(username: str, password: str) -> str | None:
    if username in USERS and USERS[username] == password:
        return username
    else:
        LOG.warning("BASIC_AUTH failed: username=%r", username)
    return None

def auth_wrapper(view):
    return AUTH.login_required(view) if AUTH_ENABLED else view
