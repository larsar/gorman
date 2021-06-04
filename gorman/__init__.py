import logging
import os

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logger = logging.getLogger('gorman')
logger.setLevel(os.environ.get('LOG_LEVEL', 'DEBUG').upper())
logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'), format=LOG_FORMAT)


def logger(name):
    l = logging.getLogger(name)
    logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'), format=LOG_FORMAT)
    return l
