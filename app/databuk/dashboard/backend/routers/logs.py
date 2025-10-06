from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any
from pathlib import Path
from core.config import settings

router = APIRouter(prefix="/logs", tags=["logs"])


def _find_latest_log_file(logs_dir: Path) -> Path | None:
    if not logs_dir.exists() or not logs_dir.is_dir():
        return None
    log_files = sorted(logs_dir.glob("*.log"), reverse=True)
    return log_files[0] if log_files else None


def _parse_log_line(line: str) -> Dict[str, Any] | None:
    # Very simple parser expecting a format like:
    # 2025-07-31T12:34:56 level=ERROR message=Something happened
    # Adjust as needed to your real log format
    line = line.strip()
    if not line:
        return None
    try:
        parts = line.split(" ")
        timestamp = parts[0]
        remainder = " ".join(parts[1:])
        level = "info"
        message = remainder
        # naive extraction
        if "level=" in remainder:
            for token in remainder.split():
                if token.lower().startswith("level="):
                    level = token.split("=", 1)[1].lower()
                    break
        if "message=" in remainder:
            message = remainder.split("message=", 1)[1]
        return {
            "id": f"{timestamp}-{abs(hash(line))}",
            "timestamp": timestamp,
            "level": "error" if level == "error" else ("warning" if level in ("warn", "warning") else "info"),
            "category": "backend",
            "message": message,
        }
    except Exception:
        return None


@router.get("")
async def get_logs(limit: int = Query(200, ge=1, le=1000)) -> Dict[str, Any]:
    """Return recent backend logs filtered to errors and warnings only."""
    try:
        latest = _find_latest_log_file(settings.LOGS_DIR)
        if latest is None:
            return {"status": "success", "logs": []}

        logs: List[Dict[str, Any]] = []
        with latest.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f.readlines()[-limit:]:
                parsed = _parse_log_line(line)
                if parsed and parsed["level"] in ("error", "warning"):
                    logs.append(parsed)

        return {"status": "success", "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read logs: {str(e)}")


