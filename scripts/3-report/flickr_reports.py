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
    return parser.parse_args()


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
        "license_total.csv",
    )

    if not os.path.exists(file_path):
        LOGGER.error(f"Data file not found: {file_path}")
        return pd.DataFrame()

    data = pd.read_csv(file_path)
    LOGGER.info(f"Data loaded from {file_path}")
    return data


def update_readme(image_path, description, section_title, args):
    """
    Update the README.md file with the generated images and descriptions.
    """
    readme_path = os.path.join(PATHS["data"], args.quarter, "README.md")
    section_marker_start = "<!-- Flickr Start -->"
    section_marker_end = "<!-- Flickr End -->"
    data_source_title = "## Data Source: Flickr"

    # Convert image path to a relative path
    rel_image_path = os.path.relpath(image_path, os.path.dirname(readme_path))

    if os.path.exists(readme_path):
        with open(readme_path, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    section_start = None
    section_end = None
    for i, line in enumerate(lines):
        if section_marker_start in line:
            section_start = i
        if section_marker_end in line:
            section_end = i

    if section_start is None or section_end is None:
        # If the section does not exist, add it at the end
        lines.append(f"\n# {args.quarter} Quantifying the Commons\n")
        lines.append(f"{section_marker_start}\n")
        lines.append(f"{data_source_title}\n\n")
        lines.append(f"{section_marker_end}\n")
        section_start = len(lines) - 3
        section_end = len(lines) - 1

    # Prepare the content to be added
    new_content = [
        f"\n### {section_title}\n",
        f"![{description}]({rel_image_path})\n",
        f"{description}\n",
    ]

    # Insert the new content before the section end marker
    lines = lines[:section_end] + new_content + lines[section_end:]

    # Write back to the README.md file
    with open(readme_path, "w") as f:
        f.writelines(lines)

    LOGGER.info(f"Updated {readme_path} with new image and description.")


# Add functions for individual license graphs + word clouds + total license


def main():

    # args = parse_arguments()

    # data = load_data(args)
    # if data.empty:
    #     return

    # current_directory = os.getcwd()
    # LOGGER.info(f"Current working directory: {current_directory}")

    LOGGER.info("Generating reports for Flickr.")
    pass


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
