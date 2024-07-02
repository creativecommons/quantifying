# Standard library
import logging
import os
from datetime import datetime, timezone

# Third-party
from pandas import PeriodIndex


def setup(current_file):
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Datetime
    datetime_today = datetime.now(timezone.utc)
    quarter = PeriodIndex([datetime_today.date()], freq="Q")[0]

    # Paths
    paths = {}
    paths["repo"] = os.path.dirname(
        os.path.abspath(os.path.realpath(os.path.join(__file__, "..")))
    )
    paths["dotenv"] = os.path.join(paths["repo"], ".env")
    paths["data"] = os.path.dirname(
        os.path.abspath(os.path.realpath(current_file))
    )
    phase = os.path.basename(
        os.path.dirname(os.path.abspath(os.path.realpath(current_file)))
    )
    paths["data"] = os.path.join(paths["repo"], "data")
    data_quarter = os.path.join(paths["data"], f"{quarter}")
    paths["state"] = os.path.join(data_quarter, "state.yaml")
    paths["data_phase"] = os.path.join(data_quarter, phase)

    paths["data_quarter"] = data_quarter

    return logger, paths


def log_paths(logger, paths):
    paths_list = []
    for label, path in paths.items():
        label = f"{label}:"
        paths_list.append(f"\n{' ' * 12}{label:<11} {path}")
    paths_list = "".join(paths_list)
    logger.info(f"PATHS:{paths_list}")
