import logging
import sys
import time
import os

class UTCFormatter(logging.Formatter):
    converter = time.gmtime

def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = "%(asctime)sZ %(levelname)s %(name)s %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%S"

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(UTCFormatter(fmt=fmt, datefmt=datefmt))

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers = [handler]
    root.propagate = False

    return handler
