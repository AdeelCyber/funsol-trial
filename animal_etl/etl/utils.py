import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_session(retries=5, backoff_factor=1, status_forcelist=(500, 502, 503, 504)):
    """
    Request session cereation which will
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def setup_logger(name="ETL", level=logging.INFO):
    """
    Logger Setup to be used during dev
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
    return logger
