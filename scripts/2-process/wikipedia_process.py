#!/usr/bin/env python
"""
This file is dedicated to processing Wikipedia data
for analysis and comparison between quarters.
"""
# Standard library
import argparse
import csv
import os
import sys
import traceback

# Third-party
# import pandas as pd
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


def data_to_csv(args, data, file_path):
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    # emulate csv.unix_dialect
    data.to_csv(
        file_path, index=False, quoting=csv.QUOTE_ALL, lineterminator="\n"
    )


def process_highest_language_usage(args, count_data):
    """
    Processing count data: top 10 highest language usage
    """
    LOGGER.info(process_highest_language_usage.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        language_name_en = row.LANGUAGE_NAME_EN
        count = row.COUNT
        data[language_name_en] = count

    data = pd.DataFrame(data.items(), columns=["language_name_en", "count"])
    data.sort_values("count", ascending=False, inplace=True)
    top_10 = data.head(10)
    file_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_highest_language_usage.csv"
    )
    data_to_csv(args, top_10, file_path)


def process_language_representation(args, count_data):
    """
    Processing count data: language representation
    """
    LOGGER.info(process_language_representation.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        language_name_en = row.LANGUAGE_NAME_EN
        count = row.COUNT
        data[language_name_en] = count

    data = pd.DataFrame(data.items(), columns=["language_name_en", "count"])
    average_count = data["count"].mean()

    data["category"] = data["count"].apply(
        lambda x: "Underrepresented" if x < average_count else "Represented"
    )
    language_counts = (
        data.groupby("category").size().reset_index(name="language_count")
    )
    language_counts.sort_values(
        "language_count", ascending=False, inplace=True
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_language_representation.csv"
    )
    data_to_csv(args, language_counts, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    file_count = shared.path_join(
        PATHS["data_1-fetch"], "wikipedia_count_by_languages.csv"
    )
    count_data = pd.read_csv(file_count, usecols=["LANGUAGE_NAME_EN", "COUNT"])
    process_highest_language_usage(args, count_data)
    process_language_representation(args, count_data)

    # Push changes
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Wikipedia data for {QUARTER}",
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
