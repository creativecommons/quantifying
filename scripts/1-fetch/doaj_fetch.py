#!/usr/bin/env python
"""
Fetch DOAJ journals with CC license information using API v4.

Note: Articles do not contain license information in DOAJ API.

Default filtering by oa_start >= 2002 to avoid false positives from journals
that retroactively adopted CC licenses. Creative Commons was founded in 2001
and first licenses released in 2002. Journals with oa_start before 2002 may
show CC licenses due to later license updates, not original terms.

Country Code Mapping:
This script requires ISO 3166-1 alpha-2 country codes for publisher analysis.
If data/iso_country_codes.yaml is missing, the script will automatically
generate it using dev/generate_country_codes.py. Users do not need to manually
create this file - it will be created programmatically when needed.
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
FILE_DOAJ_LANGUAGE = shared.path_join(
    PATHS["data_1-fetch"], "doaj_3_count_by_language.csv"
)
FILE_DOAJ_PUBLISHER = shared.path_join(
    PATHS["data_1-fetch"], "doaj_5_count_by_publisher.csv"
)
FILE_DOAJ_SUBJECT_REPORT = shared.path_join(
    PATHS["data_1-fetch"], "doaj_2_count_by_subject_report.csv"
)
FILE_PROVENANCE = shared.path_join(
    PATHS["data_1-fetch"], "doaj_provenance.yaml"
)
FILE_DOAJ_YEAR = shared.path_join(
    PATHS["data_1-fetch"], "doaj_4_count_by_year.csv"
)

# CSV Headers
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_LANGUAGE = ["TOOL_IDENTIFIER", "LANGUAGE_CODE", "LANGUAGE", "COUNT"]
HEADER_PUBLISHER = [
    "TOOL_IDENTIFIER",
    "PUBLISHER",
    "COUNTRY_CODE",
    "COUNTRY_NAME",
    "COUNT",
]
HEADER_SUBJECT_REPORT = [
    "TOOL_IDENTIFIER",
    "SUBJECT_CODE",
    "SUBJECT_LABEL",
    "COUNT",
]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]

# Language code to readable name mapping
LANGUAGE_NAMES = {
    "AF": "Afrikaans",
    "AR": "Arabic",
    "BE": "Belarusian",
    "BG": "Bulgarian",
    "BN": "Bengali",
    "CA": "Catalan",
    "CS": "Czech",
    "DA": "Danish",
    "DE": "German",
    "EL": "Greek",
    "EN": "English",
    "ES": "Spanish",
    "ET": "Estonian",
    "FA": "Persian",
    "FI": "Finnish",
    "FR": "French",
    "HE": "Hebrew",
    "HI": "Hindi",
    "HR": "Croatian",
    "HU": "Hungarian",
    "ID": "Indonesian",
    "IS": "Icelandic",
    "IT": "Italian",
    "JA": "Japanese",
    "KO": "Korean",
    "LT": "Lithuanian",
    "LV": "Latvian",
    "MK": "Macedonian",
    "MS": "Malay",
    "NL": "Dutch",
    "NO": "Norwegian",
    "PL": "Polish",
    "PT": "Portuguese",
    "RO": "Romanian",
    "RU": "Russian",
    "SK": "Slovak",
    "SL": "Slovenian",
    "SR": "Serbian",
    "SV": "Swedish",
    "SW": "Swahili",
    "TH": "Thai",
    "TR": "Turkish",
    "UK": "Ukrainian",
    "UR": "Urdu",
    "VI": "Vietnamese",
    "ZH": "Chinese",
}


# Load ISO 3166-1 alpha-2 country codes from YAML file
def load_country_names():
    """
    Load country code to name mapping from YAML file.

    Automatically generates data/iso_country_codes.yaml if missing using
    dev/generate_country_codes.py. This ensures the script is self-contained
    and does not require manual file creation by users.

    Returns:
        dict: Mapping of ISO 3166-1 alpha-2 codes to country names
    """
    country_file = shared.path_join(
        PATHS["repo"], "data", "iso_country_codes.yaml"
    )

    # Generate country codes file if it doesn't exist
    if not os.path.isfile(country_file):
        LOGGER.info("Country codes file not found, generating it...")
        generate_script = shared.path_join(
            PATHS["repo"], "dev", "generate_country_codes.py"
        )
        try:
            # Standard library
            import subprocess

            subprocess.run([sys.executable, generate_script], check=True)
            LOGGER.info("Successfully generated country codes file")
        except Exception as e:
            LOGGER.error(f"Failed to generate country codes file: {e}")
            raise shared.QuantifyingException(
                f"Critical error generating country codes: {e}", exit_code=1
            )

    try:
        with open(country_file, "r", encoding="utf-8") as fh:
            countries = yaml.safe_load(fh)
            return {country["code"]: country["name"] for country in countries}
    except Exception as e:
        LOGGER.error(f"Failed to load country codes from {country_file}: {e}")
        raise shared.QuantifyingException(
            f"Critical error loading country codes: {e}", exit_code=1
        )


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
    initialize_data_file(FILE_DOAJ_SUBJECT_REPORT, HEADER_SUBJECT_REPORT)
    initialize_data_file(FILE_DOAJ_LANGUAGE, HEADER_LANGUAGE)
    initialize_data_file(FILE_DOAJ_YEAR, HEADER_YEAR)
    initialize_data_file(FILE_DOAJ_PUBLISHER, HEADER_PUBLISHER)


def extract_license_type(license_info):
    """Extract CC license type from DOAJ license information."""
    if not license_info:
        return "UNKNOWN CC legal tool"
    for lic in license_info:
        lic_type = lic.get("type", "")
        if lic_type in CC_LICENSE_TYPES:
            return lic_type
    return "UNKNOWN CC legal tool"


def process_journals(session, args):
    """Process DOAJ journals with CC licenses using API v4."""
    LOGGER.info("Fetching DOAJ journals...")

    license_counts = Counter()
    subject_counts = defaultdict(Counter)
    language_counts = defaultdict(Counter)
    year_counts = defaultdict(Counter)
    publisher_counts = defaultdict(Counter)

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
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response.status_code == 400:
                LOGGER.info(f"Reached end of available data at page {page}")
                break
            else:
                LOGGER.error(f"Failed to fetch journals page {page}: {e}")
                raise shared.QuantifyingException(
                    f"Critical API error on page {page}: {e}", exit_code=1
                )
        except (ValueError, KeyError) as e:
            LOGGER.error(f"Failed to parse JSON response on page {page}: {e}")
            raise shared.QuantifyingException(
                f"Critical JSON parsing error on page {page}: {e}", exit_code=1
            )

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

                # Check for CC license
                license_info = bibjson.get("license")
                if not license_info:
                    continue

                license_type = extract_license_type(license_info)
                if license_type == "UNKNOWN CC legal tool":
                    continue

                license_counts[license_type] += 1

                # Extract subjects
                subjects = bibjson.get("subject", [])
                for subject in subjects:
                    if isinstance(subject, dict):
                        code = subject.get("code", "")
                        term = subject.get("term", "")
                        if code and term:
                            subject_counts[license_type][f"{code}|{term}"] += 1

                # Extract year from oa_start (Open Access start year)
                oa_start = bibjson.get("oa_start")

                # Apply date-back filter if specified
                if args.date_back and oa_start and oa_start < args.date_back:
                    continue

                if oa_start:
                    year_counts[license_type][str(oa_start)] += 1
                else:
                    year_counts[license_type]["Unknown"] += 1

                # Extract languages
                languages = bibjson.get("language", [])
                for lang in languages:
                    language_counts[license_type][lang] += 1

                # Extract publisher information (new in v4)
                publisher_info = bibjson.get("publisher", {})
                if publisher_info:
                    publisher_name = publisher_info.get("name", "Unknown")
                    publisher_country = publisher_info.get(
                        "country", "Unknown"
                    )
                    publisher_key = f"{publisher_name}|{publisher_country}"
                    publisher_counts[license_type][publisher_key] += 1

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
        subject_counts,
        language_counts,
        year_counts,
        publisher_counts,
        total_processed,
    )


def save_count_data(
    license_counts,
    subject_counts,
    language_counts,
    year_counts,
    publisher_counts,
):
    """Save all collected data to CSV files."""

    # Load country names from YAML
    country_names = load_country_names()

    # Save license counts
    with open(FILE_DOAJ_COUNT, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_COUNT, dialect="unix")
        writer.writeheader()
        for lic, count in license_counts.items():
            writer.writerow({"TOOL_IDENTIFIER": lic, "COUNT": count})

    # Save subject report
    with open(
        FILE_DOAJ_SUBJECT_REPORT, "w", encoding="utf-8", newline="\n"
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_SUBJECT_REPORT, dialect="unix"
        )
        writer.writeheader()
        for lic, subjects in subject_counts.items():
            for subject_info, count in subjects.items():
                if "|" in subject_info:
                    code, label = subject_info.split("|", 1)
                else:
                    code, label = subject_info, subject_info
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "SUBJECT_CODE": code,
                        "SUBJECT_LABEL": label,
                        "COUNT": count,
                    }
                )

    # Save language counts with readable names
    with open(FILE_DOAJ_LANGUAGE, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_LANGUAGE, dialect="unix")
        writer.writeheader()
        for lic, languages in language_counts.items():
            for lang_code, count in languages.items():
                lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "LANGUAGE_CODE": lang_code,
                        "LANGUAGE": lang_name,
                        "COUNT": count,
                    }
                )

    # Save year counts
    with open(FILE_DOAJ_YEAR, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_YEAR, dialect="unix")
        writer.writeheader()
        for lic, years in year_counts.items():
            for year, count in years.items():
                writer.writerow(
                    {"TOOL_IDENTIFIER": lic, "YEAR": year, "COUNT": count}
                )

    # Save publisher counts
    with open(FILE_DOAJ_PUBLISHER, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_PUBLISHER, dialect="unix"
        )
        writer.writeheader()
        for lic, publishers in publisher_counts.items():
            for publisher_info, count in publishers.items():
                if "|" in publisher_info:
                    publisher, country_code = publisher_info.split("|", 1)
                else:
                    publisher, country_code = publisher_info, "Unknown"

                country_name = country_names.get(country_code, country_code)
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "PUBLISHER": publisher,
                        "COUNTRY_CODE": country_code,
                        "COUNTRY_NAME": country_name,
                        "COUNT": count,
                    }
                )


def query_doaj(args):
    """Main function to query DOAJ API v4."""
    session = shared.get_session()

    LOGGER.info("Processing DOAJ journals with DOAJ API v4")

    # Process journals
    (
        license_counts,
        subject_counts,
        language_counts,
        year_counts,
        publisher_counts,
        journals_processed,
    ) = process_journals(session, args)

    # Save results
    if args.enable_save:
        save_count_data(
            license_counts,
            subject_counts,
            language_counts,
            year_counts,
            publisher_counts,
        )

    # Save provenance
    provenance_data = {
        "total_articles_fetched": 0,
        "total_journals_fetched": journals_processed,
        "total_processed": journals_processed,
        "limit": args.limit,
        "date_back_filter": args.date_back,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
        "api_version": "v4",
        "note": "Articles do not contain license information in DOAJ API",
    }

    try:
        with open(FILE_PROVENANCE, "w", encoding="utf-8", newline="\n") as fh:
            yaml.dump(provenance_data, fh, default_flow_style=False, indent=2)
    except Exception as e:
        LOGGER.error("Failed to write provenance file: %s", e)
        raise shared.QuantifyingException(
            f"Critical error writing provenance file: {e}", exit_code=1
        )

    LOGGER.info(f"Total CC licensed journals processed: {journals_processed}")
    LOGGER.info(
        "Articles: 0 (DOAJ API doesn't provide license info for articles)"
    )


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
