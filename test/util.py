import logging
import sys

from core.util import setup_logging

logger = logging.getLogger("elastic.pipes")
set_level = setup_logging()

verbosity = max(arg.count("v") for arg in sys.argv if arg.startswith("-") and not arg.startswith("--"))
if verbosity > 2:
    set_level(logging.DEBUG)
elif verbosity > 1:
    set_level(logging.INFO)
