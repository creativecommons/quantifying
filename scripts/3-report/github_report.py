#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from GitHub.
"""
# Standard library
import argparse
import os
import sys
import traceback
from datetime import datetime, timezone

# Third-party
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
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

    parser = argparse.ArgumentParser(description="GitHub Data Report")
    parser.add_argument(
        "--quarter",
        "-q",
        type=str,
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
        PATHS["data"], f"{selected_quarter}", "1-fetch", "github_fetched.csv"
    )

    if not os.path.exists(file_path):
        LOGGER.error(f"Data file not found: {file_path}")
        return pd.DataFrame()

    data = pd.read_csv(file_path)
    LOGGER.info(f"Data loaded from {file_path}")
    return data


def visualize_by_license_type(data, args):
    """
    Create a bar chart for the number of repositories licensed by license type.
    """
    LOGGER.info(
        "Creating a bar chart for the number "
        "of repositories licensed by license type."
    )

    selected_quarter = args.quarter

    # Strip any leading/trailing spaces from the columns
    data.columns = data.columns.str.strip()

    plt.figure(figsize=(12, 8))
    try:
        ax = sns.barplot(x=data["LICENSE_TYPE"], y=data["Repository Count"])
    except KeyError as e:
        LOGGER.error(f"KeyError: {e}. Available columns are: {data.columns}")
        return

    plt.title("Number of Repositories Licensed by License Type")
    plt.xlabel("License Type")
    plt.ylabel("Number of Repositories")
    plt.xticks(rotation=45, ha="right")

    # Add value numbers to the top of each bar
    for p in ax.patches:
        ax.annotate(
            format(p.get_height(), ",.0f"),
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 9),
            textcoords="offset points",
        )

    output_directory = os.path.join(
        PATHS["data"], f"{selected_quarter}", "3-report"
    )

    LOGGER.info(f"Output directory: {output_directory}")

    # Create the directory if it does not exist
    os.makedirs(output_directory, exist_ok=True)
    image_path = os.path.join(
        output_directory, "github_license_type_report.png"
    )

    plt.savefig(image_path)

    if args.show_plots:
        plt.show()

    shared.update_readme(
        PATHS,
        image_path,
        "GitHub Data",
        "Number of Repositories Licensed by License Type",
        "GitHub License Type Report",
        args,
    )

    LOGGER.info("Visualization by license type created.")


def main():

    # Fetch and merge changes
    shared.fetch_and_merge(PATHS["repo"])

    args = parse_arguments()

    data = load_data(args)
    if data.empty:
        return

    current_directory = os.getcwd()
    LOGGER.info(f"Current working directory: {current_directory}")

    visualize_by_license_type(data, args)

    # Add and commit changes
    if not args.skip_commit:
        shared.add_and_commit(
            PATHS["repo"],
            PATHS["data_quarter"],
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
