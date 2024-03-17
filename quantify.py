# Standard library
import datetime
import logging
import os.path


def setup():
    # Datetime
    datetime_today = datetime.datetime.today()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    # Paths
    path_repo_root = os.path.dirname(
        os.path.abspath(os.path.realpath(__file__))
    )
    path_dotenv = os.path.join(path_repo_root, ".env")

    return path_repo_root, path_dotenv, datetime_today, logger
