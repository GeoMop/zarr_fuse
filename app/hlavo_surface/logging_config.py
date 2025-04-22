# setup of global logging
#
# Usage in a module:
# from chodby_inv import get_logger
# logger = get_logger(__name__)
#
# def foo():
#     logger.info("Info message")
#     logger.debug("Debug message")
#
# This makes a logger that automaticaly adds the module context to the
# messages, while the log file as well as formating remains common according to the
# configuration in this file.

# logging_config.py
import logging
from datetime import datetime
from .inputs import work_dir

# ensure output directory exists
work_dir.mkdir(parents=True, exist_ok=True)

# build a timestamped filename
logfile = work_dir / f"00_processing.log"

# configure root once: just write DEBUG+ into that file
logging.basicConfig(
    filename=str(logfile),
    level=logging.DEBUG,        # only non‑default piece
    # everything else (formatter, handlers list) is left to the defaults
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger(name: str) -> logging.Logger:
    """
    Returns a module‑scoped logger, namespaced by `name`.
    The first import of this module already called basicConfig().
    """
    return logging.getLogger(name)