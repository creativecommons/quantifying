# Shared module for common functionalities across scripts

# Standard library
import datetime
import os.path


def setup():

    # Datetime
    datetime_today = datetime.datetime.today()

    # Paths
    path_work_dir = os.path.dirname(
        os.path.abspath(os.path.realpath(__file__))
    )
    path_repo_root = os.path.dirname(
        os.path.abspath(os.path.realpath(path_work_dir))
    )
    path_dotenv = os.path.abspath(
        os.path.realpath(os.path.join(path_work_dir, ".env"))
    )
    return path_repo_root, path_work_dir, path_dotenv, datetime_today
