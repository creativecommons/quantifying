#!/usr/bin/env python
"""
Process arXiv data.
"""
# Standard library
import argparse
import csv
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


def process_license_totals(args, count_data):
    """
    Processing count data: totals by license type
    """
    LOGGER.info(process_license_totals.__doc__.strip())
    data = {
        "CC BY 4.0": 0,
        "CC BY-SA 4.0": 0,
        "CC BY-NC 4.0": 0,
        "CC BY-NC-SA 4.0": 0,
        "CC BY-ND 4.0": 0,
        "CC BY-NC-ND 4.0": 0,
        "CC0 1.0": 0,
        "Public Domain": 0,
    }
    
    for row in count_data.itertuples(index=False):
        license_type = row[0]
        count = row[1]
        
        if license_type in data:
            data[license_type] += count
        else:
            # Handle variations or unknown licenses
            if "CC0" in license_type:
                data["CC0 1.0"] += count
            elif "Public Domain" in license_type:
                data["Public Domain"] += count
            else:
                # Default to CC BY 4.0 for unknown
                data["CC BY 4.0"] += count

    data = pd.DataFrame(
        data.items(), columns=["License", "Count"]
    )
    file_path = shared.path_join(PATHS["data_phase"], "arxiv_license_totals.csv")
    data_to_csv(args, data, file_path)


def process_totals_by_category(args, data):
    """
    Processing category data: totals by arXiv category
    """
    LOGGER.info(process_totals_by_category.__doc__.strip())
    data = data.groupby(["CATEGORY"], as_index=False)["COUNT"].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "CATEGORY": "Category",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_category.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_year(args, data):
    """
    Processing year data: totals by publication year
    """
    LOGGER.info(process_totals_by_year.__doc__.strip())
    data = data.groupby(["YEAR"], as_index=False)["COUNT"].sum()
    data = data.sort_values("YEAR", ascending=True)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "YEAR": "Year",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_year.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_author_count(args, data):
    """
    Processing author count data: totals by number of authors
    """
    LOGGER.info(process_totals_by_author_count.__doc__.strip())
    data = data.groupby(["AUTHOR_COUNT"], as_index=False)["COUNT"].sum()
    data = data.sort_values("AUTHOR_COUNT", ascending=True)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "AUTHOR_COUNT": "Author Count",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_author_count.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_free_cultural(args, count_data):
    """
    Processing count data: totals by Approved for Free Cultural Works
    """
    LOGGER.info(process_totals_by_free_cultural.__doc__.strip())
    data = {
        "Approved for Free Cultural Works": 0,
        "Limited use": 0,
    }
    
    for row in count_data.itertuples(index=False):
        license_type = row[0]
        count = row[1]
        
        # Free Cultural Works approved licenses
        if license_type in ["CC BY 4.0", "CC BY-SA 4.0", "CC0 1.0", "Public Domain"]:
            data["Approved for Free Cultural Works"] += count
        else:
            data["Limited use"] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    data.sort_values("Count", ascending=False, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_free_cultural.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_restrictions(args, count_data):
    """
    Processing count data: totals by restriction level
    """
    LOGGER.info(process_totals_by_restrictions.__doc__.strip())
    data = {
        "level 0 - unrestricted": 0,
        "level 1 - few restrictions": 0,
        "level 2 - some restrictions": 0,
        "level 3 - many restrictions": 0,
    }
    
    for row in count_data.itertuples(index=False):
        license_type = row[0]
        count = row[1]
        
        if license_type in ["CC0 1.0", "Public Domain"]:
            data["level 0 - unrestricted"] += count
        elif license_type in ["CC BY 4.0", "CC BY-SA 4.0"]:
            data["level 1 - few restrictions"] += count
        elif license_type in ["CC BY-NC 4.0", "CC BY-NC-SA 4.0"]:
            data["level 2 - some restrictions"] += count
        else:
            data["level 3 - many restrictions"] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_restrictions.csv"
    )
    data_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    # Count data
    file1_count = shared.path_join(PATHS["data_1-fetch"], "arxiv_1_count.csv")
    count_data = pd.read_csv(file1_count, usecols=["LICENSE", "COUNT"])
    process_license_totals(args, count_data)
    process_totals_by_free_cultural(args, count_data)
    process_totals_by_restrictions(args, count_data)

    # Category data
    file2_category = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_2_count_by_category.csv"
    )
    category_data = pd.read_csv(
        file2_category, usecols=["LICENSE", "CATEGORY", "COUNT"]
    )
    process_totals_by_category(args, category_data)

    # Year data
    file3_year = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_3_count_by_year.csv"
    )
    year_data = pd.read_csv(
        file3_year, usecols=["LICENSE", "YEAR", "COUNT"]
    )
    process_totals_by_year(args, year_data)

    # Author count data
    file4_author = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_4_count_by_author_count.csv"
    )
    author_data = pd.read_csv(
        file4_author, usecols=["LICENSE", "AUTHOR_COUNT", "COUNT"]
    )
    process_totals_by_author_count(args, author_data)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new arXiv processed data for {QUARTER}",
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
