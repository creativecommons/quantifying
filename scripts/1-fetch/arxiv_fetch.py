#!/usr/bin/env python
"""
Fetch ArXiv papers with CC license information and generate count reports.
"""
# Standard library
import argparse
import csv
import os
import re
import sys
import textwrap
import time
import traceback
import urllib.parse
from collections import Counter, defaultdict

# Third-party
import feedparser
import requests
import yaml
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
# API Configuration
BASE_URL = "http://export.arxiv.org/api/query?"
API_DELAY_SECONDS = 3  # ArXiv recommended delay between API calls
RESULTS_PER_REQUEST = 50  # Number of results per API request
MAX_RESULTS_PER_QUERY = 500  # Maximum results to fetch per search query
DEFAULT_FETCH_LIMIT = 800  # Default total papers to fetch

# HTTP Retry Configuration (using shared constants where available)
RETRY_TOTAL = 5
RETRY_BACKOFF_FACTOR = 1


# Search Queries
SEARCH_QUERIES = [
    'all:"creative commons"',
    'all:"CC BY"',
    'all:"CC-BY"',
    'all:"CC BY-NC"',
    'all:"CC-BY-NC"',
    'all:"CC BY-SA"',
    'all:"CC-BY-SA"',
    'all:"CC BY-ND"',
    'all:"CC-BY-ND"',
    'all:"CC BY-NC-SA"',
    'all:"CC-BY-NC-SA"',
    'all:"CC BY-NC-ND"',
    'all:"CC-BY-NC-ND"',
    'all:"CC0"',
    'all:"CC 0"',
    'all:"CC-0"',
]

# File Paths
FILE_ARXIV_COUNT = shared.path_join(PATHS["data_1-fetch"], "arxiv_1_count.csv")
FILE_ARXIV_CATEGORY_REPORT = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_2_count_by_category_report.csv"
)
FILE_ARXIV_CATEGORY_REPORT_AGGREGATE = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_2_count_by_category_report_agg.csv"
)
FILE_ARXIV_YEAR = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_3_count_by_year.csv"
)
FILE_ARXIV_AUTHOR_BUCKET = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_4_count_by_author_bucket.csv"
)
# records metadata for each run for audit, reproducibility, and provenance
FILE_PROVENANCE = shared.path_join(PATHS["data"], "arxiv_provenance.yaml")

HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_CATEGORY_REPORT = [
    "TOOL_IDENTIFIER",
    "CATEGORY_CODE",
    "CATEGORY_LABEL",
    "COUNT",
    "PERCENT",
]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]
HEADER_AUTHOR_BUCKET = ["TOOL_IDENTIFIER", "AUTHOR_BUCKET", "COUNT"]

QUARTER = os.path.basename(PATHS["data_quarter"])

CATEGORY_LABELS = {}

# Compiled regex patterns for CC license detection
CC_PATTERNS = [
    (re.compile(r"\bCC[-\s]?0\b", re.IGNORECASE), "CC0"),
    (
        re.compile(r"\bCC[-\s]?BY[-\s]?NC[-\s]?ND\b", re.IGNORECASE),
        "CC BY-NC-ND",
    ),
    (
        re.compile(r"\bCC[-\s]?BY[-\s]?NC[-\s]?SA\b", re.IGNORECASE),
        "CC BY-NC-SA",
    ),
    (re.compile(r"\bCC[-\s]?BY[-\s]?ND\b", re.IGNORECASE), "CC BY-ND"),
    (re.compile(r"\bCC[-\s]?BY[-\s]?SA\b", re.IGNORECASE), "CC BY-SA"),
    (re.compile(r"\bCC[-\s]?BY[-\s]?NC\b", re.IGNORECASE), "CC BY-NC"),
    (re.compile(r"\bCC[-\s]?BY\b", re.IGNORECASE), "CC BY"),
    (
        re.compile(r"\bCREATIVE\s+COMMONS\b", re.IGNORECASE),
        "UNKNOWN CC legal tool",
    ),
]

# Log the start of the script execution
LOGGER.info("Script execution started.")


# parsing arguments function
def parse_arguments():
    """Parse command-line options, returns parsed argument namespace."""
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=f"Limit papers to fetch (default: {DEFAULT_FETCH_LIMIT})",
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
        with open(file_path, "w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=headers, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """Initialize all data files used by this script.

    Creates the data directory and initializes empty CSVs with headers.
    """
    if not args.enable_save:
        return

    os.makedirs(PATHS["data_1-fetch"], exist_ok=True)
    initialize_data_file(FILE_ARXIV_COUNT, HEADER_COUNT)
    initialize_data_file(FILE_ARXIV_CATEGORY_REPORT, HEADER_CATEGORY_REPORT)
    initialize_data_file(FILE_ARXIV_YEAR, HEADER_YEAR)
    initialize_data_file(FILE_ARXIV_AUTHOR_BUCKET, HEADER_AUTHOR_BUCKET)


def get_requests_session():
    """Create request session with retry logic"""
    retry_strategy = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": shared.USER_AGENT})
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def normalize_license_text(raw_text):
    """
    Convert raw license text to standardized CC license identifiers.

    Uses regex patterns to identify CC licenses from paper text.
    Returns specific license (e.g., "CC BY", "CC0") or "Unknown".
    """
    if not raw_text:
        return "Unknown"

    for pattern, license_type in CC_PATTERNS:
        if pattern.search(raw_text):
            return license_type

    return "Unknown"


def extract_license_info(entry):
    """
    Extract CC license information from ArXiv paper entry.

    Checks rights field first, then summary field for license patterns.
    Returns normalized license identifier or "Unknown".
    """
    # checking through the rights field first then summary
    if hasattr(entry, "rights") and entry.rights:
        license_info = normalize_license_text(entry.rights)
        if license_info != "Unknown":
            return license_info
    if hasattr(entry, "summary") and entry.summary:
        license_info = normalize_license_text(entry.summary)
        if license_info != "Unknown":
            return license_info
    return "Unknown"


def extract_category_from_entry(entry):
    """Extract primary category from ArXiv entry."""
    if (
        hasattr(entry, "arxiv_primary_category")
        and entry.arxiv_primary_category
    ):
        return entry.arxiv_primary_category.get("term", "Unknown")
    if hasattr(entry, "tags") and entry.tags:
        # Get first category from tags
        for tag in entry.tags:
            if hasattr(tag, "term"):
                return tag.term
    return "Unknown"


def extract_year_from_entry(entry):
    """Extract publication year from ArXiv entry."""
    if hasattr(entry, "published") and entry.published:
        try:
            return entry.published[:4]  # Extract year from date string
        except (AttributeError, IndexError) as e:
            LOGGER.debug(
                f"Failed to extract year from '{entry.published}': {e}"
            )
    return "Unknown"


def extract_author_count_from_entry(entry):
    """Extract number of authors from ArXiv entry."""
    if hasattr(entry, "authors") and entry.authors:
        try:
            return len(entry.authors)
        except Exception as e:
            LOGGER.debug(f"Failed to count authors from entry.authors: {e}")
    if hasattr(entry, "author") and entry.author:
        return 1
    return "Unknown"


def bucket_author_count(n):
    """
    Convert author count to predefined buckets for analysis.

    Buckets: "1", "2-3", "4-6", "7-10", "11+", "Unknown"
    Reduces granularity for better statistical analysis.
    """
    if n is None:
        return "Unknown"
    if n == 1:
        return "1"
    if 2 <= n <= 3:
        return "2-3"
    if 4 <= n <= 6:
        return "4-6"
    if 7 <= n <= 10:
        return "7-10"
    return "11+"


def save_count_data(
    license_counts, category_counts, year_counts, author_counts
):
    """
    Save all collected data to CSV files.

    """
    # license_counts: {license: count}
    # category_counts: {license: {category_code: count}}
    # year_counts: {license: {year: count}}
    # author_counts: {license: {author_count(int|None): count}}

    # Save license counts
    with open(FILE_ARXIV_COUNT, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_COUNT, dialect="unix")
        writer.writeheader()
        for lic, c in license_counts.items():
            writer.writerow({"TOOL_IDENTIFIER": lic, "COUNT": c})

    # Save category report with labels and percent
    with open(
        FILE_ARXIV_CATEGORY_REPORT, "w", newline="", encoding="utf-8"
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_CATEGORY_REPORT, dialect="unix"
        )
        writer.writeheader()
        for lic, cats in category_counts.items():
            total_for_license = sum(cats.values()) or 1
            for code, c in cats.items():
                label = shared.normalize_arxiv_category(code, CATEGORY_LABELS)
                pct = round((c / total_for_license) * 100, 2)
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "CATEGORY_CODE": code,
                        "CATEGORY_LABEL": label,
                        "COUNT": c,
                        "PERCENT": pct,
                    }
                )

    # Save aggregated category report (top N per license, rest -> Other)
    TOP_N = 10
    with open(
        FILE_ARXIV_CATEGORY_REPORT_AGGREGATE, "w", newline="", encoding="utf-8"
    ) as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "TOOL_IDENTIFIER",
                "CATEGORY_CODE",
                "CATEGORY_LABEL",
                "COUNT",
                "PERCENT",
            ],
            dialect="unix",
        )
        writer.writeheader()
        for lic, cats in category_counts.items():
            total_for_license = sum(cats.values()) or 1
            sorted_cats = sorted(
                cats.items(), key=lambda x: x[1], reverse=True
            )
            top = sorted_cats[:TOP_N]
            others = sorted_cats[TOP_N:]
            other_count = sum(c for _, c in others)
            for code, c in top:
                label = shared.normalize_arxiv_category(code, CATEGORY_LABELS)
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "CATEGORY_CODE": code,
                        "CATEGORY_LABEL": label,
                        "COUNT": c,
                        "PERCENT": round((c / total_for_license) * 100, 2),
                    }
                )
            if other_count:
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "CATEGORY_CODE": "OTHER",
                        "CATEGORY_LABEL": "Other",
                        "COUNT": other_count,
                        "PERCENT": round(
                            (other_count / total_for_license) * 100, 2
                        ),
                    }
                )

    # Save year counts
    with open(FILE_ARXIV_YEAR, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_YEAR, dialect="unix")
        writer.writeheader()
        for lic, years in year_counts.items():
            for year, c in years.items():
                writer.writerow(
                    {"TOOL_IDENTIFIER": lic, "YEAR": year, "COUNT": c}
                )

    # Save author buckets summary
    with open(
        FILE_ARXIV_AUTHOR_BUCKET, "w", newline="", encoding="utf-8"
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_AUTHOR_BUCKET, dialect="unix"
        )
        writer.writeheader()
        # build buckets across licenses
        for lic, acs in author_counts.items():
            bucket_counts = Counter()
            for ac, c in acs.items():
                b = bucket_author_count(ac)
                bucket_counts[b] += c
            for b, c in bucket_counts.items():
                writer.writerow(
                    {"TOOL_IDENTIFIER": lic, "AUTHOR_BUCKET": b, "COUNT": c}
                )


def query_arxiv(args):
    """
    Main function to query ArXiv API and collect CC license data.

    """

    LOGGER.info("Beginning to fetch results from ArXiv API")
    session = get_requests_session()

    # Load category mappings using shared function
    CATEGORY_LABELS.update(shared.load_arxiv_categories(PATHS.get("data")))

    results_per_iteration = RESULTS_PER_REQUEST

    search_queries = SEARCH_QUERIES

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
            0,
            min(args.limit - total_fetched, MAX_RESULTS_PER_QUERY),
            results_per_iteration,
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
                time.sleep(API_DELAY_SECONDS)
            except requests.RequestException as e:
                LOGGER.error(f"Request failed: {e}")
                break

            if papers_found_in_batch == 0:
                consecutive_empty_calls += 1
                if consecutive_empty_calls >= 2:
                    LOGGER.info(
                        f"No new papers in 2 consecutive calls for "
                        f"query: {search_query}. "
                        f"Moving over to the next query."
                    )
                    break
            else:
                consecutive_empty_calls = 0

    # Save results
    if args.enable_save:
        save_count_data(
            license_counts, category_counts, year_counts, author_counts
        )

    # save provenance
    provenance_data = {
        "total_fetched": total_fetched,
        "queries": search_queries,
        "limit": args.limit,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
    }

    # write provenance YAML for auditing
    try:
        os.makedirs(os.path.dirname(FILE_PROVENANCE), exist_ok=True)
        with open(FILE_PROVENANCE, "w", encoding="utf-8") as fh:
            yaml.dump(provenance_data, fh, default_flow_style=False, indent=2)
    except Exception as e:
        LOGGER.warning("Failed to write provenance file: %s", e)

    LOGGER.info(f"Total CC licensed papers fetched: {total_fetched}")


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
