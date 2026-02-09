#!/usr/bin/env python
"""
This file is dedicated to processing GitHub data
for analysis and comparison between quarters.
"""

# Standard library
import argparse
import os
import sys
import traceback

# Third-party
import pandas as pd

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
FILE_PATHS = [
    shared.path_join(PATHS["data_phase"], "github_totals_by_license.csv"),
    shared.path_join(PATHS["data_phase"], "github_totals_by_restriction.csv"),
]


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    global QUARTER
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=QUARTER,
        help=f"Data quarter in format YYYYQx (default: {QUARTER})",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results (default: False)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions such as fetch, merge, add, commit, and push"
        " (default: False)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate data even if processed files already exist",
    )

    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        global FILE_PATHS, PATHS
        FILE_PATHS = shared.paths_list_update(
            LOGGER, FILE_PATHS, QUARTER, args.quarter
        )
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
        QUARTER = args.quarter
    args.logger = LOGGER
    args.paths = PATHS
    return args


def process_totals_by_license(args, count_data):
    """
    Processing count data: totals by License
    """
    LOGGER.info(process_totals_by_license.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        tool = str(row.TOOL_IDENTIFIER)
        count = int(row.COUNT)

        if tool == "Total public repositories":
            continue

        data[tool] = count

    data = pd.DataFrame(data.items(), columns=["License", "Count"])
    data.sort_values("License", ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "github_totals_by_license.csv"
    )
    shared.dataframe_to_csv(args, data, file_path)


def process_totals_by_restriction(args, count_data):
    """
    Processing count data: totals by restriction
    """
    # https://creativecommons.org/public-domain/freeworks/
    LOGGER.info(process_totals_by_restriction.__doc__.strip())
    data = {"Copyleft": 0, "Permissive": 0, "Public domain": 0}

    for row in count_data.itertuples(index=False):
        tool = str(row.TOOL_IDENTIFIER)
        count = int(row.COUNT)

        if tool == "Total public repositories":
            continue

        if tool in ["BSD Zero Clause License", "CC0 1.0", "Unlicense"]:
            key = "Public domain"
        elif tool in ["MIT No Attribution", "CC BY 4.0"]:
            key = "Permissive"
        elif tool in ["CC BY-SA 4.0"]:
            key = "Copyleft"
        else:
            continue

        data[key] += count
    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    data.sort_values("Category", ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "github_totals_by_restriction.csv"
    )
    shared.dataframe_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    shared.check_completion_file_exists(args, FILE_PATHS)
    file_count = shared.path_join(PATHS["data_1-fetch"], "github_1_count.csv")
    count_data = shared.open_data_file(
        LOGGER, file_count, usecols=["TOOL_IDENTIFIER", "COUNT"]
    )
    process_totals_by_license(args, count_data)
    process_totals_by_restriction(args, count_data)

    # Push changes
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new GitHub data for {QUARTER}",
    )
    shared.git_push_changes(args, PATHS["repo"])


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
