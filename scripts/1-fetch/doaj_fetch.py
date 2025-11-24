#!/usr/bin/env python
"""
Fetch DOAJ journals with CC license information using API v4.

Focus: Journal-level CC license adoption and temporal trends.
Note: Articles do not contain license information in DOAJ API.

This script focuses on essential data for quantifying Creative Commons adoption:
- Journal CC license counts by type
- Temporal trends (year-by-year adoption)

Removed out-of-scope data: subjects, languages, publishers, countries.

Default filtering by oa_start >= 2002 to avoid false positives from journals
that retroactively adopted CC licenses. Creative Commons was founded in 2001
and first licenses released in 2002. Journals with oa_start before 2002 may
show CC licenses due to later license updates, not original terms.
"""
# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback
from collections import Counter, defaultdict

# Third-party
import pycountry
import requests
import yaml
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
BASE_URL = "https://doaj.org/api/v4/search"
DEFAULT_DATE_BACK = 2002  # Creative Commons licenses first released in 2002
DEFAULT_FETCH_LIMIT = 1000
RATE_LIMIT_DELAY = 0.5

# CC License types
CC_LICENSE_TYPES = [
    "CC BY",
    "CC BY-NC",
    "CC BY-SA",
    "CC BY-ND",
    "CC BY-NC-SA",
    "CC BY-NC-ND",
    "CC0",
    "UNKNOWN CC legal tool",
]

# File Paths
FILE_DOAJ_COUNT = shared.path_join(PATHS["data_1-fetch"], "doaj_1_count.csv")
FILE_DOAJ_COUNTRY = shared.path_join(PATHS["data_1-fetch"], "doaj_3_count_by_country.csv")
FILE_DOAJ_LANGUAGE = shared.path_join(PATHS["data_1-fetch"], "doaj_5_count_by_language.csv")
FILE_PROVENANCE = shared.path_join(
    PATHS["data_1-fetch"], "doaj_provenance.yaml"
)
FILE_DOAJ_YEAR = shared.path_join(
    PATHS["data_1-fetch"], "doaj_4_count_by_year.csv"
)

# CSV Headers
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_COUNTRY = ["TOOL_IDENTIFIER", "COUNTRY_CODE", "COUNTRY_NAME", "COUNT"]
HEADER_LANGUAGE = ["TOOL_IDENTIFIER", "LANGUAGE_CODE", "LANGUAGE_NAME", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]

# Runtime variables
QUARTER = os.path.basename(PATHS["data_quarter"])


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch DOAJ journals with CC licenses using API v4"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=f"Total journals to fetch (default: {DEFAULT_FETCH_LIMIT})",
    )
    parser.add_argument(
        "--date-back",
        type=int,
        default=DEFAULT_DATE_BACK,
        help=f"Only include journals with oa_start year >= this value "
        f"(default: {DEFAULT_DATE_BACK}). Set to 2002 to avoid false "
        f"positives from journals that retroactively adopted CC licenses "
        f"after Creative Commons was established. Journals starting "
        f"before 2002 may show CC licenses due to later updates, not "
        f"original licensing terms.",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving data to CSV files",
    )
    parser.add_argument(
        "--enable-git", action="store_true", help="Enable git actions"
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def initialize_data_file(file_path, headers):
    """Initialize CSV file with headers if it doesn't exist."""
    if not os.path.isfile(file_path):
        with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=headers, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """Initialize all data files."""
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_1-fetch"], exist_ok=True)
    initialize_data_file(FILE_DOAJ_COUNT, HEADER_COUNT)
    initialize_data_file(FILE_DOAJ_COUNTRY, HEADER_COUNTRY)
    initialize_data_file(FILE_DOAJ_LANGUAGE, HEADER_LANGUAGE)
    initialize_data_file(FILE_DOAJ_YEAR, HEADER_YEAR)


def get_country_name(country_code):
    """Get country name from ISO 3166-1 alpha-2 code using pycountry."""
    if not country_code or country_code == "Unknown":
        return "Unknown"
    try:
        country = pycountry.countries.get(alpha_2=country_code.upper())
        return country.name if country else country_code
    except Exception:
        return country_code


def get_language_name(language_code):
    """Get language name from ISO 639-1 code using pycountry."""
    if not language_code or language_code == "Unknown":
        return "Unknown"
    try:
        language = pycountry.languages.get(alpha_2=language_code.upper())
        return language.name if language else language_code
    except Exception:
        return language_code


def extract_license_types(license_info):
    """Extract all CC license types from DOAJ license information."""
    if not license_info:
        return []
    
    cc_licenses = []
    for lic in license_info:
        lic_type = lic.get("type", "")
        if lic_type in CC_LICENSE_TYPES:
            cc_licenses.append(lic_type)
    
    return cc_licenses


def process_journals(session, args):
    """Process DOAJ journals with CC licenses using API v4."""
    LOGGER.info("Fetching DOAJ journals...")

    license_counts = Counter()
    country_counts = defaultdict(Counter)
    language_counts = defaultdict(Counter)
    year_counts = defaultdict(Counter)
    processed_journals = set()  # Track unique journals to avoid double counting

    total_processed = 0
    page = 1
    page_size = 100

    while total_processed < args.limit:
        LOGGER.info(f"Fetching journals page {page}...")

        url = f"{BASE_URL}/journals/*"
        params = {"pageSize": page_size, "page": page}

        try:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.HTTPError as e:
            raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
        except requests.RequestException as e:
            raise shared.QuantifyingException(f"Request Exception: {e}", 1)
        except KeyError as e:
            raise shared.QuantifyingException(f"KeyError: {e}", 1)

        try:
            results = data.get("results", [])
            if not results:
                break
        except (AttributeError, TypeError) as e:
            LOGGER.error(f"Invalid API response structure on page {page}: {e}")
            raise shared.QuantifyingException(
                f"Critical API response format error on page {page}: {e}",
                exit_code=1,
            )

        for journal in results:
            if total_processed >= args.limit:
                break

            try:
                bibjson = journal.get("bibjson", {})

                # Get journal identifier to avoid double counting
                journal_id = journal.get("id", "")
                if not journal_id:
                    continue

                # Check for CC licenses
                license_info = bibjson.get("license")
                if not license_info:
                    continue

                cc_license_types = extract_license_types(license_info)
                if not cc_license_types:
                    continue

                # Extract year from oa_start (Open Access start year)
                oa_start = bibjson.get("oa_start")

                # Apply date-back filter if specified
                if args.date_back and oa_start and oa_start < args.date_back:
                    continue

                # Count each license type this journal supports
                for license_type in cc_license_types:
                    license_counts[license_type] += 1

                    # Add year data for each license type
                    if oa_start:
                        year_counts[license_type][str(oa_start)] += 1
                    else:
                        year_counts[license_type]["Unknown"] += 1

                    # Extract country information
                    publisher_info = bibjson.get("publisher", {})
                    if isinstance(publisher_info, dict):
                        country_code = publisher_info.get("country", "Unknown")
                        country_counts[license_type][country_code] += 1
                    
                    # Extract language information
                    languages = bibjson.get("language", [])
                    if languages:
                        for lang_code in languages:
                            language_counts[license_type][lang_code] += 1
                    else:
                        language_counts[license_type]["Unknown"] += 1

                # Track unique journals to avoid double counting in statistics
                if journal_id not in processed_journals:
                    processed_journals.add(journal_id)

                total_processed += 1

            except (KeyError, AttributeError, TypeError) as e:
                LOGGER.warning(
                    f"Skipping malformed journal record on page {page}: {e}"
                )
                continue
            except Exception as e:
                LOGGER.error(
                    f"Unexpected error processing journal on page {page}: {e}"
                )
                raise shared.QuantifyingException(
                    f"Critical error processing journal data on page {page}: "
                    f"{e}",
                    exit_code=1,
                )

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return (
        license_counts,
        country_counts,
        language_counts,
        year_counts,
        len(processed_journals),  # Return unique journal count
    )


def save_count_data(
    license_counts,
    country_counts,
    language_counts,
    year_counts,
):
    """Save essential journal data to CSV files."""

    # Save license counts
    with open(
        FILE_DOAJ_COUNT, "w", encoding="utf-8", newline="\n"
    ) as file_object:
        writer = csv.DictWriter(
            file_object, fieldnames=HEADER_COUNT, dialect="unix"
        )
        writer.writeheader()
        for lic, count in license_counts.items():
            writer.writerow({"TOOL_IDENTIFIER": lic, "COUNT": count})

    # Save country counts with pycountry names
    with open(
        FILE_DOAJ_COUNTRY, "w", encoding="utf-8", newline="\n"
    ) as file_object:
        writer = csv.DictWriter(
            file_object, fieldnames=HEADER_COUNTRY, dialect="unix"
        )
        writer.writeheader()
        for lic, countries in country_counts.items():
            for country_code, count in countries.items():
                country_name = get_country_name(country_code)
                writer.writerow({
                    "TOOL_IDENTIFIER": lic,
                    "COUNTRY_CODE": country_code,
                    "COUNTRY_NAME": country_name,
                    "COUNT": count,
                })

    # Save language counts with pycountry names
    with open(
        FILE_DOAJ_LANGUAGE, "w", encoding="utf-8", newline="\n"
    ) as file_object:
        writer = csv.DictWriter(
            file_object, fieldnames=HEADER_LANGUAGE, dialect="unix"
        )
        writer.writeheader()
        for lic, languages in language_counts.items():
            for lang_code, count in languages.items():
                lang_name = get_language_name(lang_code)
                writer.writerow({
                    "TOOL_IDENTIFIER": lic,
                    "LANGUAGE_CODE": lang_code,
                    "LANGUAGE_NAME": lang_name,
                    "COUNT": count,
                })

    # Save year counts
    with open(
        FILE_DOAJ_YEAR, "w", encoding="utf-8", newline="\n"
    ) as file_object:
        writer = csv.DictWriter(
            file_object, fieldnames=HEADER_YEAR, dialect="unix"
        )
        writer.writeheader()
        for lic, years in year_counts.items():
            for year, count in years.items():
                writer.writerow(
                    {"TOOL_IDENTIFIER": lic, "YEAR": year, "COUNT": count}
                )


def query_doaj(args):
    """Main function to query DOAJ API v4."""
    session = shared.get_session()

    LOGGER.info("Processing DOAJ journals with DOAJ API v4")

    # Process journals
    (
        license_counts,
        country_counts,
        language_counts,
        year_counts,
        journals_processed,
    ) = process_journals(session, args)

    # Save results
    if args.enable_save:
        save_count_data(
            license_counts,
            country_counts,
            language_counts,
            year_counts,
        )

    # Save provenance
    provenance_data = {
        "total_journals_fetched": journals_processed,
        "total_processed": journals_processed,
        "limit": args.limit,
        "date_back_filter": args.date_back,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
        "api_version": "v4",
        "note": "Journal-level CC license data only - article counts not available via DOAJ API",
    }

    try:
        with open(
            FILE_PROVENANCE, "w", encoding="utf-8", newline="\n"
        ) as file_object:
            yaml.dump(
                provenance_data,
                file_object,
                default_flow_style=False,
                indent=2,
            )
    except Exception as e:
        LOGGER.error("Failed to write provenance file: %s", e)
        raise shared.QuantifyingException(
            f"Critical error writing provenance file: {e}", exit_code=1
        )

    LOGGER.info(f"Unique CC-licensed journals processed: {journals_processed}")
    
    # Calculate total license availability instances
    total_license_instances = sum(license_counts.values())
    LOGGER.info(f"Total CC license type instances: {total_license_instances}")
    LOGGER.info("Note: Journals supporting multiple CC license types are counted once per license type")


def main():
    """Main function."""
    LOGGER.info("Script execution started.")
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    query_doaj(args)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new DOAJ CC license data for {QUARTER} using API v4",
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
