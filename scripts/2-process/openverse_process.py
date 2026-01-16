#!/usr/bin/env python
"""
This file is dedicated to processing Openverse data
for analysis and comparison between quarters.
"""
# Standard library
import argparse
import csv
import os
import sys
import traceback
from collections import defaultdict

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


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
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
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        global PATHS
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
    args.logger = LOGGER
    args.paths = PATHS
    return args


def check_for_data_file(file_path):
    if os.path.exists(file_path):
        raise shared.QuantifyingException(
            f"Processed data already exists for {QUARTER}", 0
        )


def data_to_csv(args, data, file_path):
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    # emulate csv.unix_dialect
    data.to_csv(
        file_path, index=False, quoting=csv.QUOTE_ALL, lineterminator="\n"
    )


def process_totals_by_license(args, count_data):
    """
    Processing count data: totals by license
    """
    LOGGER.info(process_totals_by_license.__doc__.strip())
    data = defaultdict(int)

    for row in count_data.itertuples(index=False):
        tool = str(row.TOOL_IDENTIFIER)
        count = int(row.MEDIA_COUNT)

        data[tool] += count
    data = pd.DataFrame(data.items(), columns=["License", "Count"])
    data.sort_values("License", ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "openverse_totals_by_license.csv"
    )
    check_for_data_file(file_path)
    data_to_csv(args, data, file_path)


def process_totals_by_media_type(args, count_data):
    """
    Processing count data: totals by media type
    """

    LOGGER.info(process_totals_by_media_type.__doc__.strip())
    data = defaultdict(int)

    for row in count_data.itertuples(index=False):
        media_type = str(row.MEDIA_TYPE)
        count = int(row.MEDIA_COUNT)

        data[media_type] += count
    data = pd.DataFrame(data.items(), columns=["Media_type", "Count"])
    data.sort_values("Media_type", ascending=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "openverse_totals_by_media_type.csv"
    )
    check_for_data_file(file_path)
    data_to_csv(args, data, file_path)


def process_totals_by_source(args, count_data):
    """
    Processing count data: totals by source
    """
    LOGGER.info(process_totals_by_source.__doc__.strip())
    data = defaultdict(int)
    for row in count_data.itertuples(index=False):
        source = str(row.SOURCE)
        count = int(row.MEDIA_COUNT)

        data[source] += count
    data = pd.DataFrame(data.items(), columns=["Source", "Count"])
    data.sort_values("Source", ascending=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "openverse_totals_by_source.csv"
    )
    check_for_data_file(file_path)
    data_to_csv(args, data, file_path)


def process_permissive_by_media_type(args, count_data):
    """
    Processing count data: permissive by media type
    """
    LOGGER.info(process_permissive_by_media_type.__doc__.strip())

    data = defaultdict(int)

    for row in count_data.itertuples(index=False):
        tool = str(row.TOOL_IDENTIFIER)
        media_type = str(row.MEDIA_TYPE)
        count = int(row.MEDIA_COUNT)

        if tool in ["CC0", "CC BY", "CC BY-SA"]:
            data[media_type] += count

    data = pd.DataFrame(data.items(), columns=["Media_type", "Count"])
    data.sort_values("Media_type", ascending=True, inplace=True)

    file_path = shared.path_join(
        PATHS["data_phase"], "openverse_permissive_by_media_type.csv"
    )
    check_for_data_file(file_path)
    data_to_csv(args, data, file_path)


def process_permissive_by_source(args, count_data):
    """
    Processing count data: permissive content by source
    """
    LOGGER.info(process_permissive_by_source.__doc__.strip())
    data = defaultdict(int)
    for row in count_data.itertuples(index=False):
        tool = str(row.TOOL_IDENTIFIER)
        source = str(row.SOURCE)
        count = int(row.MEDIA_COUNT)
        if tool in ["CC0", "CC BY", "CC BY-SA"]:
            data[source] += count
    data = pd.DataFrame(data.items(), columns=["Source", "Count"])
    data.sort_values("Source", ascending=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "openverse_permissive_by_source.csv"
    )
    check_for_data_file(file_path)
    data_to_csv(args, data, file_path)


def process_totals_by_restriction(args, count_data):
    """
    Processing count data: totals by restriction
    """
    # https://creativecommons.org/public-domain/freeworks/
    LOGGER.info(process_totals_by_restriction.__doc__.strip())

    data = {
        "Copyleft": 0,
        "Permissive": 0,
        "Public domain": 0,
        "Restricted": 0,
    }

    for row in count_data.itertuples(index=False):
        tool = str(row.TOOL_IDENTIFIER)
        count = int(row.MEDIA_COUNT)

        if tool in ["CC0", "PDM"]:
            key = "Public domain"

        elif tool in ["CC BY"]:
            key = "Permissive"

        elif tool in ["CC BY-SA"]:
            key = "Copyleft"

        else:
            key = "Restricted"

        data[key] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    data.sort_values("Category", ascending=True, inplace=True)

    file_path = shared.path_join(
        PATHS["data_phase"], "openverse_totals_by_restriction.csv"
    )
    check_for_data_file(file_path)
    data_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    file_count = shared.path_join(PATHS["data_1-fetch"], "openverse_fetch.csv")
    count_data = shared.open_data_file(
        LOGGER,
        file_count,
        usecols=["SOURCE", "MEDIA_TYPE", "TOOL_IDENTIFIER", "MEDIA_COUNT"],
    )
    process_totals_by_license(args, count_data)
    process_totals_by_media_type(args, count_data)
    process_totals_by_source(args, count_data)
    process_permissive_by_media_type(args, count_data)
    process_permissive_by_source(args, count_data)
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
        sys.exit(e.code)
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
