# Standard library
import datetime
import logging
import os


def setup(current_file):
    # Datetime
    datetime_today = datetime.datetime.today()

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Paths
    path_repo_root = os.path.dirname(
        os.path.abspath(os.path.realpath(__file__))
    )
    path_dotenv = os.path.join(path_repo_root, ".env")
    path_work_dir = os.path.dirname(
        os.path.abspath(os.path.realpath(current_file))
    )

    return path_repo_root, path_work_dir, path_dotenv, datetime_today, logger
