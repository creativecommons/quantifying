#!/usr/bin/env python
"""
This file is dedicated to visualizing the data collected for Flickr.
"""
# Standard library
import argparse
import os
import sys
import traceback
from datetime import datetime, timezone

# Third-party
# import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
import pandas as pd

# import seaborn as sns
from pandas import PeriodIndex

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")

    # Taken from shared module, fix later
    datetime_today = datetime.now(timezone.utc)
    quarter = PeriodIndex([datetime_today.date()], freq="Q")[0]

    parser = argparse.ArgumentParser(description="Flickr Report")
    parser.add_argument(
        "--quarter",
        "-q",
        type=str,
        required=False,
        default=f"{quarter}",
        help="Data quarter in format YYYYQx, e.g., 2024Q2",
    )
    parser.add_argument(
        "--skip-commit",
        action="store_true",
        help="Don't git commit changes (also skips git push changes)",
    )
    parser.add_argument(
        "--skip-push",
        action="store_true",
        help="Don't git push changes",
    )
    parser.add_argument(
        "--show-plots",
        action="store_true",
        help="Show generated plots (in addition to saving them)",
    )
    args = parser.parse_args()
    if args.skip_commit:
        args.skip_push = True
    return args


def load_data(args):
    """
    Load the collected data from the CSV file.
    """
    selected_quarter = args.quarter

    file_path = os.path.join(
        PATHS["data"],
        f"{selected_quarter}",
        "1-fetch",
        "flickr_fetched",
        "final.csv",
    )

    if not os.path.exists(file_path):
        LOGGER.error(f"Data file not found: {file_path}")
        return pd.DataFrame()

    data = pd.read_csv(file_path)
    LOGGER.info(f"Data loaded from {file_path}")
    return data


# Add functions for individual license graphs + word clouds + total license


def main():

    # Fetch and merge changes
    shared.fetch_and_merge(PATHS["repo"])

    args = parse_arguments()

    data = load_data(args)
    if data.empty:
        return

    current_directory = os.getcwd()
    LOGGER.info(f"Current working directory: {current_directory}")

    """
    Insert functions for Flickr
    """

    # Add and commit changes
    if not args.skip_commit:
        shared.add_and_commit(
            PATHS["repo"],
            PATHS["data_phase"],
            "Add and commit new GitHub reports",
        )

    # Push changes
    if not args.skip_push:
        shared.push_changes(PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
