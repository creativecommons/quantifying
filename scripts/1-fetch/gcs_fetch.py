#!/usr/bin/env python
"""
Fetch CC Legal Tool usage data from Google Custom Search (GCS) API.
"""

# Standard library
import argparse
import csv
import os
import random
import sys
import textwrap
import time
import traceback
import urllib.parse
from copy import copy

# Third-party
import googleapiclient.discovery
from googleapiclient.errors import HttpError
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
BASE_URL = "https://www.googleapis.com/customsearch/v1"
FILE1_COUNT = shared.path_join(PATHS["data_phase"], "gcs_1_count.csv")
FILE2_LANGUAGE = shared.path_join(
    PATHS["data_phase"], "gcs_2_count_by_language.csv"
)
FILE3_COUNTRY = shared.path_join(
    PATHS["data_phase"], "gcs_3_count_by_country.csv"
)
GCS_CX = os.getenv("GCS_CX")
GCS_DEVELOPER_KEY = os.getenv("GCS_DEVELOPER_KEY")
HEADER1_COUNT = ["PLAN_INDEX", "TOOL_IDENTIFIER", "COUNT"]
HEADER2_LANGUAGE = ["PLAN_INDEX", "TOOL_IDENTIFIER", "LANGUAGE", "COUNT"]
HEADER3_COUNTRY = ["PLAN_INDEX", "TOOL_IDENTIFIER", "COUNTRY", "COUNT"]
PLAN_COMPLETED_INDEX = 2868
QUARTER = os.path.basename(PATHS["data_quarter"])

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Limit queries (default: 1)",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, and push)",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: avoid hitting API (generate fake data)",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_search_service():
    """
    Creates and returns the Google Custom Search API service.
    """
    LOGGER.info("Getting Google Custom Search API Service.")
    return googleapiclient.discovery.build(
        "customsearch",
        "v1",
        developerKey=GCS_DEVELOPER_KEY,
        cache_discovery=False,
        num_retries=5,
    )


def initialize_all_data_files(args):
    for file_path, header in [
        (FILE1_COUNT, HEADER1_COUNT),
        (FILE2_LANGUAGE, HEADER2_LANGUAGE),
        (FILE3_COUNTRY, HEADER3_COUNTRY),
    ]:
        if not os.path.isfile(file_path):
            shared.rows_to_csv(args, file_path, header, [])


def get_last_completed_plan_index():
    last_completed_plan_index = 0
    for file_path in [FILE1_COUNT, FILE2_LANGUAGE, FILE3_COUNTRY]:
        try:
            with open(file_path, "r", encoding="utf-8") as file_obj:
                reader = csv.DictReader(file_obj, dialect="unix")
                for row in reader:
                    pass  # skip through to last row
                try:
                    last_completed_plan_index = max(
                        last_completed_plan_index,
                        int(row["PLAN_INDEX"]),
                    )
                except UnboundLocalError:
                    pass  # Data row may not be found with --enable-save, etc.
        except FileNotFoundError:
            pass  # File may not be found without --enable-save, etc.
    LOGGER.info(f"Last completed plan index: {last_completed_plan_index}")
    return last_completed_plan_index


def load_plan():
    plan = []
    file_path = shared.path_join(PATHS["data"], "gcs_query_plan.csv")
    with open(file_path, "r", encoding="utf-8") as file_obj:
        plan = list(csv.DictReader(file_obj, dialect="unix"))
    return plan


def append_data(args, plan_row, index, count):
    if plan_row["COUNTRY"]:
        file_path = FILE3_COUNTRY
        fieldnames = HEADER3_COUNTRY
        row = {
            "PLAN_INDEX": index,
            "TOOL_IDENTIFIER": plan_row["TOOL_IDENTIFIER"],
            "COUNTRY": plan_row["COUNTRY"],
            "COUNT": count,
        }
    elif plan_row["LANGUAGE"]:
        file_path = FILE2_LANGUAGE
        fieldnames = HEADER2_LANGUAGE
        row = {
            "PLAN_INDEX": index,
            "TOOL_IDENTIFIER": plan_row["TOOL_IDENTIFIER"],
            "LANGUAGE": plan_row["LANGUAGE"],
            "COUNT": count,
        }
    else:
        file_path = FILE1_COUNT
        fieldnames = HEADER1_COUNT
        row = {
            "PLAN_INDEX": index,
            "TOOL_IDENTIFIER": plan_row["TOOL_IDENTIFIER"],
            "COUNT": count,
        }
    shared.rows_to_csv(args, file_path, fieldnames, [row], append=True)


def query_gcs(args, service, last_completed_plan_index, plan):
    LOGGER.info(
        "Beginning to fetch results from Google Custom Search (GCS) API"
    )

    max_tries = 5
    initial_delay = 1  # in seconds
    if args.dev:
        # query development "API" as fast as possible
        rate_delay = 0
    else:
        # query production API gently
        rate_delay = copy(initial_delay)
    start = last_completed_plan_index
    stop = start + args.limit
    for plan_row in plan[start:stop]:  # noqa: E203
        index = plan.index(plan_row) + 1
        query_info = f"index: {index}, tool: {plan_row['TOOL_IDENTIFIER']}"
        # Note that the URL is quoted, which improves accuracy
        # https://blog.google/products/search/how-were-improving-search-results-when-you-use-quotes/
        encoded_tool_url = urllib.parse.quote(
            f'"{plan_row["TOOL_URL"]}"', safe=":/"
        )
        query_params = {
            "cx": GCS_CX,
            "linkSite": plan_row["TOOL_URL"].lstrip("/"),
            "q": encoded_tool_url,
        }
        if plan_row["COUNTRY"]:
            query_info = f"{query_info}, country: {plan_row['COUNTRY']}"
            query_params["cr"] = plan_row["CR"]
        elif plan_row["LANGUAGE"]:
            query_info = f"{query_info}, language: {plan_row['LANGUAGE']}"
            query_params["lr"] = plan_row["LR"]

        success = False
        for attempt in range(max_tries):
            LOGGER.info(f"Query: {query_info}")
            try:
                if args.dev:
                    results = {
                        "searchInformation": {
                            "totalResults": random.randint(666000, 666999)
                        }
                    }
                else:
                    results = service.cse().list(**query_params).execute()
                count = int(
                    results.get("searchInformation", {}).get("totalResults", 0)
                )
                success = True
                time.sleep(rate_delay)
                break  # no need to try again

            except HttpError as e:
                if e.status_code == 429:
                    if (
                        "Quota exceeded" in e.reason
                        and "Queries per day" in e.reason
                    ):
                        LOGGER.warning(f"{e.status_code}: {e.reason}")
                        return  # abort queries
                    else:
                        LOGGER.warning(
                            f"{e.status_code}: {e.reason}. retrying in"
                            f" {initial_delay} seconds"
                        )
                        time.sleep(initial_delay)
                        initial_delay *= 2  # Exponential backoff
                else:
                    LOGGER.error(f"Error fetching results: {e}")
                    break
        if success:
            append_data(args, plan_row, index, count)
        else:
            LOGGER.error(
                "Max tries exceeded. Could not complete request (plan index"
                f" {index})."
            )
            return  # abort queries


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    service = get_search_service()
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    last_completed_plan_index = get_last_completed_plan_index()
    if last_completed_plan_index == PLAN_COMPLETED_INDEX:
        LOGGER.info(f"Data fetch completed for {QUARTER}")
        return
    plan = load_plan()
    query_gcs(args, service, last_completed_plan_index, plan)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Google Custom Search (GCS) data for {QUARTER}",
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
