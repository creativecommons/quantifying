#!/usr/bin/env python
"""
Process ArXiv data.
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
    return parser.parse_args()


def data_to_csv(args, data, file_path):
    """Save DataFrame to CSV if save is enabled."""
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    # emulate csv.unix_dialect
    data.to_csv(
        file_path, index=False, quoting=csv.QUOTE_ALL, lineterminator="\n"
    )


def process_license_totals(args, count_data):
    """
    Processing count data: totals by license
    """
    LOGGER.info(process_license_totals.__doc__.strip())
    data = count_data.groupby(["TOOL_IDENTIFIER"], as_index=False)[
        "COUNT"
    ].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "TOOL_IDENTIFIER": "License",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_license.csv"
    )
    data_to_csv(args, data, file_path)


def process_category_totals(args, category_data):
    """
    Processing category data: totals by category
    """
    LOGGER.info(process_category_totals.__doc__.strip())
    data = category_data.groupby(["CATEGORY_CODE", "CATEGORY_LABEL"], as_index=False)[
        "COUNT"
    ].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)

    data.rename(
        columns={
            "CATEGORY_CODE": "Category_Code",
            "CATEGORY_LABEL": "Category_Name",
            "COUNT": "Count",
        },
        inplace=True,
    )
    # Reorder columns to have Code, Name, Count
    data = data[["Category_Code", "Category_Name", "Count"]]

    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_category.csv"
    )
    data_to_csv(args, data, file_path)


def process_year_totals(args, year_data):
    """
    Processing year data: totals by year
    """
    LOGGER.info(process_year_totals.__doc__.strip())
    data = year_data.groupby(["YEAR"], as_index=False)["COUNT"].sum()
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


def process_author_bucket_totals(args, author_data):
    """
    Processing author bucket data: totals by author bucket
    """
    LOGGER.info(process_author_bucket_totals.__doc__.strip())
    # Filter out rows with empty/null author buckets
    author_data = author_data.dropna(subset=["AUTHOR_BUCKET"])
    author_data = author_data[author_data["AUTHOR_BUCKET"].str.strip() != ""]

    if author_data.empty:
        LOGGER.warning("No valid author bucket data found")
        # Create empty DataFrame with proper structure
        data = pd.DataFrame(columns=["Author_Bucket", "Count"])
    else:
        data = author_data.groupby(["AUTHOR_BUCKET"], as_index=False)[
            "COUNT"
        ].sum()
        # Define bucket order for proper sorting
        bucket_order = ["1", "2", "3", "4", "5+", "Unknown"]
        data["AUTHOR_BUCKET"] = pd.Categorical(
            data["AUTHOR_BUCKET"], categories=bucket_order, ordered=True
        )
        data = data.sort_values("AUTHOR_BUCKET")
        data.reset_index(drop=True, inplace=True)
        data.rename(
            columns={
                "AUTHOR_BUCKET": "Author_Bucket",
                "COUNT": "Count",
            },
            inplace=True,
        )

    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_author_bucket.csv"
    )
    data_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    # Count data
    file1_count = shared.path_join(PATHS["data_1-fetch"], "arxiv_1_count.csv")
    count_data = pd.read_csv(file1_count, usecols=["TOOL_IDENTIFIER", "COUNT"])
    process_license_totals(args, count_data)

    # Category data
    file2_category = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_2_count_by_category_report.csv"
    )
    category_data = pd.read_csv(
        file2_category, usecols=["CATEGORY_CODE", "CATEGORY_LABEL", "COUNT"]
    )
    process_category_totals(args, category_data)

    # Year data
    file3_year = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_3_count_by_year.csv"
    )
    year_data = pd.read_csv(file3_year, usecols=["YEAR", "COUNT"])
    process_year_totals(args, year_data)

    # Author bucket data
    file4_author = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_4_count_by_author_bucket.csv"
    )
    author_data = pd.read_csv(file4_author, usecols=["AUTHOR_BUCKET", "COUNT"])
    process_author_bucket_totals(args, author_data)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new ArXiv data for {QUARTER}",
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
