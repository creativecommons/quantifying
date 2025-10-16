#!/usr/bin/env python
"""
Fetch ArXiv papers with CC license information and generate count reports.
"""
# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback
import urllib.parse
from collections import defaultdict

# Third-party
import feedparser
import requests
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
BASE_URL = "http://export.arxiv.org/api/query?"
FILE_ARXIV_COUNT = shared.path_join(PATHS["data_1-fetch"], "arxiv_1_count.csv")
FILE_ARXIV_CATEGORY = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_2_count_by_category.csv"
)
FILE_ARXIV_YEAR = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_3_count_by_year.csv"
)
FILE_ARXIV_AUTHOR = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_4_count_by_author_count.csv"
)

HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_CATEGORY = ["TOOL_IDENTIFIER", "CATEGORY", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]
HEADER_AUTHOR = ["TOOL_IDENTIFIER", "AUTHOR_COUNT", "COUNT"]

QUARTER = os.path.basename(PATHS["data_quarter"])

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """Parse command-line options, returns parsed argument namespace."""
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=800,
        help="Limit number of papers to fetch (default: 800)",
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
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def initialize_data_file(file_path, headers):
    """Initialize CSV file with headers if it doesn't exist."""
    if not os.path.isfile(file_path):
        with open(file_path, "w", newline="") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=headers, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """Initialize all data files."""
    if not args.enable_save:
        return

    os.makedirs(PATHS["data_1-fetch"], exist_ok=True)
    initialize_data_file(FILE_ARXIV_COUNT, HEADER_COUNT)
    initialize_data_file(FILE_ARXIV_CATEGORY, HEADER_CATEGORY)
    initialize_data_file(FILE_ARXIV_YEAR, HEADER_YEAR)
    initialize_data_file(FILE_ARXIV_AUTHOR, HEADER_AUTHOR)


def extract_license_info(entry):
    """Extract CC license information from ArXiv entry."""
    license_info = "Unknown"

    # Check for license in rights field
    if hasattr(entry, "rights") and entry.rights:
        license_info = entry.rights.upper()
        if "CC0" in license_info or "CC 0" in license_info:
            license_info = "CC0"
        elif "CC BY-NC-ND" in license_info:
            license_info = "CC BY-NC-ND"
        elif "CC BY-NC-SA" in license_info:
            license_info = "CC BY-NC-SA"
        elif "CC BY-ND" in license_info:
            license_info = "CC BY-ND"
        elif "CC BY-SA" in license_info:
            license_info = "CC BY-SA"
        elif "CC BY-NC" in license_info:
            license_info = "CC BY-NC"
        elif "CC BY" in license_info:
            license_info = "CC BY"
        elif "CREATIVE COMMONS" in license_info:
            license_info = "Creative Commons"
        else:
            license_info = "unknown"

    # Check for license in summary/abstract
    if license_info == "Unknown" and hasattr(entry, "summary"):
        license_info = entry.summary.upper()
        if "CCO" in license_info or "CC 0" in license_info:
            license_info = "CC0"
        elif "CC BY-NC-ND" in license_info:
            license_info = "CC BY-NC-ND"
        elif "CC BT-NC-SA" in license_info:
            license_info = "CC BY-NC-SA"
        elif "CC BY-ND" in license_info:
            license_info = "CC BY-ND"
        elif "CC BY-SA" in license_info:
            license_info = "CC BY-SA"
        elif "CC BY-NC" in license_info:
            license_info = "CC BY-NC"
        elif "CC BY" in license_info:
            license_info = "CC BY"
        elif "CREATIVE COMMONS" in license_info:
            license_info = "Creative Commons"
        else:
            license_info = "Unknown"

    return license_info


def extract_category_from_entry(entry):
    """Extract primary category from ArXiv entry."""
    if (
        hasattr(entry, "arxiv_primary_category")
        and entry.arxiv_primary_category
    ):
        return entry.arxiv_primary_category.get("term", "Unknown")
    elif hasattr(entry, "tags") and entry.tags:
        # Get first category from tags
        for tag in entry.tags:
            if hasattr(tag, "term"):
                return tag.term
    return "Unknown"


def extract_year_from_entry(entry):
    """Extract publication year from ArXiv entry."""
    if hasattr(entry, "published"):
        try:
            return entry.published[:4]  # Extract year from date string
        except (AttributeError, IndexError):
            pass
    return "Unknown"


def extract_author_count_from_entry(entry):
    """Extract number of authors from ArXiv entry."""
    if hasattr(entry, "authors"):
        return str(len(entry.authors))
    elif hasattr(entry, "author"):
        return "1"
    return "Unknown"


def get_requests_session():
    """Create requests session with retry logic."""
    retry_strategy = Retry(
        total=5,
        backoff_factor=3,
        status_forcelist=[408, 429, 500, 502, 503, 504],
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def query_arxiv(args):
    """Query ArXiv API for papers with potential CC licenses."""

    LOGGER.info("Beginning to fetch results from ArXiv API")

    session = get_requests_session()
    results_per_iteration = 50

    search_queries = [
        'all:"creative commons"',
        'all:"CC BY"',
        'all:"CC BY-NC"',
        'all:"CC BY-SA"',
        'all:"CC BY-ND"',
        'all:"CC BY-NC-SA"',
        'all:"CC BY-NC-ND"',
        'all:"CC0"',
    ]

    # Data structures for counting
    license_counts = defaultdict(int)
    category_counts = defaultdict(lambda: defaultdict(int))
    year_counts = defaultdict(lambda: defaultdict(int))
    author_counts = defaultdict(lambda: defaultdict(int))

    total_fetched = 0

    for search_query in search_queries:
        if total_fetched >= args.limit:
            break

        LOGGER.info(f"Searching for: {search_query}")
        consecutive_empty_calls = 0

        for start in range(
            0, min(args.limit - total_fetched, 500), results_per_iteration
        ):
            encoded_query = urllib.parse.quote_plus(search_query)
            query = (
                f"search_query={encoded_query}&start={start}"
                f"&max_results={results_per_iteration}"
            )

            papers_found_in_batch = 0

            try:
                LOGGER.info(
                    f"Fetching results {start} - "
                    f"{start + results_per_iteration}"
                )
                response = session.get(BASE_URL + query, timeout=30)
                response.raise_for_status()
                feed = feedparser.parse(response.content)

                for entry in feed.entries:
                    if total_fetched >= args.limit:
                        break

                    license_info = extract_license_info(entry)

                    if license_info != "Unknown":
                        category = extract_category_from_entry(entry)
                        year = extract_year_from_entry(entry)
                        author_count = extract_author_count_from_entry(entry)

                        # Count by license
                        license_counts[license_info] += 1

                        # Count by category and license
                        category_counts[license_info][category] += 1

                        # Count by year and license
                        year_counts[license_info][year] += 1

                        # Count by author count and license
                        author_counts[license_info][author_count] += 1

                        total_fetched += 1
                        papers_found_in_batch += 1

                        LOGGER.info(
                            f"Found CC licensed paper: {license_info} - "
                            f"{category} - {year}"
                        )

                # arXiv recommends a 3-seconds delay between consecutive
                # api calls for efficiency
                time.sleep(3)
            except requests.RequestException as e:
                LOGGER.error(f"Request failed: {e}")
                break

        if papers_found_in_batch == 0:
            consecutive_empty_calls += 1
            if consecutive_empty_calls >= 2:
                LOGGER.info(
                    f"No new papers in 2 consecutive calls for "
                    f"query: {search_query}. Moving over to the next query."
                )
                break
        else:
            consecutive_empty_calls = 0

    # Save results

    if args.enable_save:
        save_count_data(
            license_counts, category_counts, year_counts, author_counts
        )

    LOGGER.info(f"Total CC licensed papers fetched: {total_fetched}")


def save_count_data(
    license_counts, category_counts, year_counts, author_counts
):
    """Save count data to CSV files."""
    # Save license counts
    with open(FILE_ARXIV_COUNT, "w", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_COUNT, dialect="unix"
        )
        writer.writeheader()

        for license_type, count in license_counts.items():
            writer.writerow(
                {
                    "TOOL_IDENTIFIER": license_type,
                    "COUNT": count,
                }
            )

    # Save category counts
    with open(FILE_ARXIV_CATEGORY, "w", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_CATEGORY, dialect="unix"
        )
        writer.writeheader()

        for license_type, categories in category_counts.items():
            for category, count in categories.items():
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": license_type,
                        "CATEGORY": category,
                        "COUNT": count,
                    }
                )

    # Save year counts
    with open(FILE_ARXIV_YEAR, "w", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_YEAR, dialect="unix"
        )
        writer.writeheader()

        for license_type, years in year_counts.items():
            for year, count in years.items():
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": license_type,
                        "YEAR": year,
                        "COUNT": count,
                    }
                )

    # Save author count data
    with open(FILE_ARXIV_AUTHOR, "w", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_AUTHOR, dialect="unix"
        )
        writer.writeheader()

        for license_type, author_counts_data in author_counts.items():
            for author_count, count in author_counts_data.items():
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": license_type,
                        "AUTHOR_COUNT": author_count,
                        "COUNT": count,
                    }
                )


def main():
    """Main function."""
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    query_arxiv(args)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new ArXiv CC license data for {QUARTER}",
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
