import logging
import os
import sys
import time

COLORS = {
    "DEBUG": "\033[36m",     # cyan
    "INFO": "\033[32m",      # green
    "WARNING": "\033[33m",   # yellow
    "ERROR": "\033[31m",     # red
    "CRITICAL": "\033[41m",  # red background
}

RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    converter = time.gmtime

    def format(self, record):
        level_color = COLORS.get(record.levelname, "")
        record.levelname = f"{level_color}{record.levelname}{RESET}"
        return super().format(record)


def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    fmt = "%(asctime)sZ %(levelname)s %(name)s %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%S"

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColorFormatter(fmt=fmt, datefmt=datefmt))

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(handler)
