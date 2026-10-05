#!/usr/bin/env python
"""
This file is dedicated to processing Wikipedia data
for analysis and comparison between quarters.
"""

# Standard library
import argparse
import os
import sys
import textwrap
import traceback

# Third-party
import pandas as pd
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
FILE_PATHS = [
    shared.path_join(
        PATHS["data_phase"], "wikipedia_highest_language_usage.csv"
    ),
    shared.path_join(
        PATHS["data_phase"], "wikipedia_least_language_usage.csv"
    ),
    shared.path_join(
        PATHS["data_phase"], "wikipedia_language_representation.csv"
    ),
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


def process_highest_language_usage(args, count_data):
    """
    Processing count data: Most represented languages
    """
    LOGGER.info(process_highest_language_usage.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        Language = row.LANGUAGE_NAME_EN
        Count = row.COUNT
        data[Language] = Count

    data = pd.DataFrame(data.items(), columns=["Language", "Count"])
    data.sort_values("Count", ascending=False, inplace=True)
    top_10 = data.head(10)
    file_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_highest_language_usage.csv"
    )
    shared.dataframe_to_csv(args, top_10, file_path)


def process_least_language_usage(args, count_data):
    """
    Processing count data: Least represented languages
    """
    LOGGER.info(process_least_language_usage.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        Language = row.LANGUAGE_NAME_EN
        Count = row.COUNT

        if Count >= 1:
            data[Language] = Count

    data = pd.DataFrame(data.items(), columns=["Language", "Count"])
    data.sort_values("Count", ascending=True, inplace=True)
    bottom_10 = data.head(10)
    file_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_least_language_usage.csv"
    )
    shared.dataframe_to_csv(args, bottom_10, file_path)


def process_language_representation(args, count_data):
    """
    Processing count data: Language representation
    """
    LOGGER.info(process_language_representation.__doc__.strip())
    data = {}

    for row in count_data.itertuples(index=False):
        Language = row.LANGUAGE_NAME_EN
        Count = row.COUNT
        data[Language] = Count

    data = pd.DataFrame(data.items(), columns=["Language", "Count"])
    average_count = data["Count"].mean()

    data["Category"] = data["Count"].apply(
        lambda x: "Underrepresented" if x < average_count else "Represented"
    )
    language_counts = data.groupby("Category").size().reset_index(name="Count")
    language_counts.sort_values("Count", ascending=False, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_language_representation.csv"
    )
    shared.dataframe_to_csv(args, language_counts, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    shared.check_completion_file_exists(args, FILE_PATHS)
    file_count = shared.path_join(
        PATHS["data_1-fetch"], "wikipedia_count_by_languages.csv"
    )
    count_data = shared.open_data_file(
        LOGGER, file_count, usecols=["LANGUAGE_NAME_EN", "COUNT"]
    )
    process_language_representation(args, count_data)
    process_highest_language_usage(args, count_data)
    process_least_language_usage(args, count_data)

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
        sys.exit(e.exit_code)
    except SystemExit as e:
        if e.code != 0:
            LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        traceback_formatted = textwrap.indent(
            highlight(
                traceback.format_exc(),
                PythonTracebackLexer(),
                TerminalFormatter(),
            ),
            "    ",
        )
        LOGGER.critical(f"(1) Unhandled exception:\n{traceback_formatted}")
        sys.exit(1)
