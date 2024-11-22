#!/usr/bin/env python
"""
This file is dedicated to querying data from the Google Custom Search API.
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

# Third-party
import googleapiclient.discovery
from dotenv import load_dotenv
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

# Load environment variables
load_dotenv(PATHS["dotenv"])

# Constants
DEVELOPER_KEY = os.getenv("GCS_DEVELOPER_KEY")
CX = os.getenv("GCS_CX")
BASE_URL = "https://www.googleapis.com/customsearch/v1"
FILE1_COUNT = os.path.join(PATHS["data_phase"], "gcs_1_count.csv")
FILE2_LANGUAGE = os.path.join(
    PATHS["data_phase"], "gcs_2_count_by_language.csv"
)
FILE3_COUNTRY = os.path.join(PATHS["data_phase"], "gcs_3_count_by_country.csv")
HEADER1_COUNT = ["PLAN_INDEX", "TOOL_IDENTIFIER", "COUNT"]
HEADER2_LANGUAGE = ["PLAN_INDEX", "TOOL_IDENTIFIER", "LANGUAGE", "COUNT"]
HEADER3_COUNTRY = ["PLAN_INDEX", "TOOL_IDENTIFIER", "COUNTRY", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description="Google Custom Search Script")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: avoid hitting API (generates fake data)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, and push)",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Limit queries (default: 1)",
    )
    return parser.parse_args()


def get_search_service():
    """
    Creates and returns the Google Custom Search API service.
    """
    LOGGER.info("Getting Google Custom Search API Service.")
    return googleapiclient.discovery.build(
        "customsearch", "v1", developerKey=DEVELOPER_KEY, cache_discovery=False
    )


def initialize_data_file(file_path, header):
    if not os.path.isfile(file_path):
        with open(file_path, "w", newline="") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=header, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files():
    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    initialize_data_file(FILE1_COUNT, HEADER1_COUNT)
    initialize_data_file(FILE2_LANGUAGE, HEADER2_LANGUAGE)
    initialize_data_file(FILE3_COUNTRY, HEADER3_COUNTRY)


def get_last_completed_plan_index():
    last_completed_plan_index = 0
    for file_path in [FILE1_COUNT, FILE2_LANGUAGE, FILE3_COUNTRY]:
        with open(file_path, "r", newline="") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            for row in reader:
                pass  # skip through to last row
            try:
                last_completed_plan_index = max(
                    last_completed_plan_index,
                    int(row["PLAN_INDEX"]),
                )
            except UnboundLocalError:
                pass
    LOGGER.info(f"Last completed plan index: {last_completed_plan_index}")
    return last_completed_plan_index


def load_plan():
    path = []
    file_path = os.path.join(PATHS["data"], "gcs_query_plan.csv")
    with open(file_path, "r", newline="") as file_obj:
        path = list(csv.DictReader(file_obj, dialect="unix"))
    return path


def append_data(args, plan_row, index, count):
    if not args.enable_save:
        return
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
    with open(file_path, "a", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=fieldnames, dialect="unix"
        )
        writer.writerow(row)


def query_gcs(args, service, last_completed_plan_index, plan):
    LOGGER.info(
        "Beginning to fetch results from Google Custom Search (GCS) API"
    )

    max_tries = 5
    initial_delay = 1  # in seconds
    start = last_completed_plan_index + 1
    stop = start + args.limit

    for plan_row in plan[start:stop]:  # noqa: E203
        index = plan.index(plan_row)
        query_info = f"index: {index}, tool: {plan_row['TOOL_IDENTIFIER']}"
        encoded_tool_url = urllib.parse.quote(plan_row["TOOL_URL"], safe=":/")
        query_params = {
            "cx": CX,
            # "num": records_per_query,
            # "start": start_index,
            # "cr": cr,
            # "lr": lr,
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
                break  # no need to try again

            except HttpError as e:
                if e.status_code == 429:
                    if (
                        "Quota exceeded" in e.reason
                        and "Queries per day" in e.reason
                    ):
                        LOGGER.warning(f"{e.status_code}: {e.reason}.")
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
    shared.log_paths(LOGGER, PATHS)
    service = get_search_service()
    initialize_all_data_files()
    last_completed_plan_index = get_last_completed_plan_index()
    if last_completed_plan_index == 2867:
        LOGGER.info(f"Data fetch completed for {QUARTER}")
        return
    plan = load_plan()
    query_gcs(args, service, last_completed_plan_index, plan)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        "Add and commit new Google Custom Search (GCS) data for" f" {QUARTER}",
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
        traceback_formatted = textwrap.indent(
            highlight(
                traceback.format_exc(),
                PythonTracebackLexer(),
                TerminalFormatter(),
            ),
            "    ",
        )
        LOGGER.exception(f"(1) Unhandled exception:\n{traceback_formatted}")
        sys.exit(1)
