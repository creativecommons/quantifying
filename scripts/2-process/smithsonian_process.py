#!/usr/bin/env python
"""
This file is dedicated to processing Smithsonian data
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
    shared.path_join(PATHS["data_phase"], "smithsonian_totals_by_units.csv"),
    shared.path_join(PATHS["data_phase"], "smithsonian_totals_by_records.csv"),
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


def process_totals_by_units(args, count_data):
    """
    Processing count data: totals by units
    """
    LOGGER.info(process_totals_by_units.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        unit = str(row.UNIT)
        total_objects = int(row.TOTAL_OBJECTS)

        data[unit] = total_objects

    data = pd.DataFrame(data.items(), columns=["Unit", "Count"])
    data.sort_values("Unit", ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_totals_by_units.csv"
    )
    shared.data_to_csv(args, data, file_path)


def process_totals_by_records(args, count_data):
    """
    Processing count data: totals by records
    """
    LOGGER.info(process_totals_by_records.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        unit = str(row.UNIT)
        cc0_records = int(row.CC0_RECORDS)
        cc0_records_with_cc0_media = int(row.CC0_RECORDS_WITH_CC0_MEDIA)
        total_objects = int(row.TOTAL_OBJECTS)

        if cc0_records == 0 and cc0_records_with_cc0_media == 0:
            continue

        if unit not in data:
            data[unit] = {
                "CC0_RECORDS": 0,
                "CC0_RECORDS_WITH_CC0_MEDIA": 0,
                "TOTAL_OBJECTS": 0,
            }

        data[unit]["CC0_RECORDS"] += cc0_records
        data[unit]["CC0_RECORDS_WITH_CC0_MEDIA"] += cc0_records_with_cc0_media
        data[unit]["TOTAL_OBJECTS"] += total_objects

    data = (
        pd.DataFrame.from_dict(data, orient="index")
        .reset_index()
        .rename(columns={"index": "Unit"})
    )
    data["CC0_RECORDS_PERCENTAGE"] = (
        (data["CC0_RECORDS"] / data["TOTAL_OBJECTS"]) * 100
    ).round(2)

    data["CC0_RECORDS_WITH_CC0_MEDIA_PERCENTAGE"] = (
        (data["CC0_RECORDS_WITH_CC0_MEDIA"] / data["TOTAL_OBJECTS"]) * 100
    ).round(2)

    data.sort_values("Unit", ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)

    file_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_totals_by_records.csv"
    )
    shared.data_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    shared.check_completion_file_exists(args, FILE_PATHS)
    file_count = shared.path_join(
        PATHS["data_1-fetch"], "smithsonian_2_units.csv"
    )
    count_data = shared.open_data_file(
        LOGGER,
        file_count,
        usecols=[
            "UNIT",
            "CC0_RECORDS",
            "CC0_RECORDS_WITH_CC0_MEDIA",
            "TOTAL_OBJECTS",
        ],
    )
    process_totals_by_units(args, count_data)
    process_totals_by_records(args, count_data)

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
