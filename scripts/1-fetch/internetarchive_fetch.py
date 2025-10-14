#!/usr/bin/env python
"""
Fetch public domain data from Internet Archive using the Python interface.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
from collections import Counter
from time import sleep

# Third-party
from internetarchive.session import ArchiveSession
from internetarchive.search import Search
from internetarchive import search_items
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
FILE1_COUNT = os.path.join(PATHS["data_phase"], "internetarchive_1_count.csv")
FILE2_LANGUAGE = os.path.join(PATHS["data_phase"], "internetarchive_2_count_by_language.csv")
FILE3_COUNTRY = os.path.join(PATHS["data_phase"], "internetarchive_3_count_by_country.csv")

HEADER1 = ["LICENSEURL", "LICENSE", "COUNT"]
HEADER2 = ["LICENSEURL", "LICENSE", "LANGUAGE", "COUNT"]
HEADER3 = ["LICENSEURL", "LICENSE", "COUNTRY", "COUNT"]

QUARTER = os.path.basename(PATHS["data_quarter"])

def parse_arguments():
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enable-save", action="store_true", help="Enable saving results")
    parser.add_argument("--enable-git", action="store_true", help="Enable git actions")
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args

def _normalize_url(url):
    """A helper to standardize a URL string for matching."""
    if not isinstance(url, str) or not url:
        return None
    # Chain all transformations for maximum robustness
    normalized = (
        url.lower()                     # Convert to lowercase
        .strip()                        # Remove leading/trailing whitespace
        .replace("http://", "https://") # Standardize to http
        .replace("www.", "")            # Remove 'www.' subdomain
        .split("/legalcode")[0]         # Remove /legalcode suffix
        .rstrip("/")                    # Remove trailing slash
    )
    return normalized

def _normalize_url(url):
    """A helper to standardize a URL string for matching."""
    if not isinstance(url, str) or not url:
        return None

    # 1. Basic cleaning
    normalized = (
        url.lower()
        .strip()
        .replace("http://", "https://")
        .replace("www.", "")
    )

    # 2. Remove common suffixes like /legalcode or /deed
    normalized = normalized.split("/legalcode")[0]
    normalized = normalized.split("/deed")[0]
    normalized = normalized.split("/ch")[0]

    # 3. Truncate extra path segments (e.g., language codes like /de, /es)
    parts = normalized.split("/")
    # A standard base license URL has 5 parts, e.g., ['http:', '', 'creativecommons.org', 'licenses', 'by-nc-nd-4.0']
    # This logic truncates anything longer than that.
    if "licenses" in parts and len(parts) > 5:
        normalized = "/".join(parts[:5])

    # 4. Final cleanup of any trailing slash
    normalized = normalized.rstrip("/")

    return normalized

def load_license_mapping():
    """Loads and normalizes the license mapping from CSV."""
    mapping = {}
    file_path = shared.path_join(PATHS["data"], "ia_license_mapping.csv")
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use the new helper to create a clean key
            normalized_url = _normalize_url(row["LICENSEURL"])
            if normalized_url:
                label = row["LICENSE"].strip()
                mapping[normalized_url] = label
    return mapping

def normalize_license(licenseurl, license_mapping):
    """Finds the license label for a given URL using the normalized mapping."""
    normalized_url = _normalize_url(licenseurl)
    # If the URL is empty or invalid after normalization, return UNKNOWN
    if not normalized_url:
        return "UNKNOWN"
    return license_mapping.get(normalized_url, "UNKNOWN")

def query_internet_archive(args):
    license_counter = Counter()
    language_counter = Counter()
    country_counter = Counter()

    session = ArchiveSession()
    fields = ["licenseurl", "language", "country"]
    query = "text:creativecommons.org"
    license_mapping = load_license_mapping()

    rows = 100000
    total_processed = 0
    max_retries = 3

    while True:
        LOGGER.info(f"Fetching {rows} items...")
        for attempt in range(max_retries):
            try:
                search = session.search_items(
                    query,
                    fields=fields,
                    params={"rows": rows},
                    request_kwargs={"timeout": 90}
                )

                results = list(search)
                break
            except Exception as e:
                wait_time = 2 ** attempt
                sleep(wait_time)
        else:
            LOGGER.error(f"Failed to data after {max_retries} attempts. Skipping.")
            sleep(2.0)
            continue

        if not results:
            LOGGER.info("No more results. Ending pagination.")
            break

        for i, result in enumerate(results):
            licenseurl = result.get("licenseurl", "")
            if isinstance(licenseurl, list):
                licenseurl = licenseurl[0] if licenseurl else "UNKNOWN"
            licenseurl = licenseurl.lower().strip()

            language = result.get("language", "UNKNOWN")
            if isinstance(language, list):
                language = language[0] if language else "UNKNOWN"

            country = result.get("country", "UNKNOWN")
            if isinstance(country, list):
                country = country[0] if country else "UNKNOWN"

            normalized_license = normalize_license(licenseurl, license_mapping)
            if normalized_license == "UNKNOWN":
                LOGGER.warning(f"Unmapped license URL: {licenseurl}")

            license_counter[(licenseurl, normalized_license)] += 1
            language_counter[(licenseurl, normalized_license, language)] += 1
            country_counter[(licenseurl, normalized_license, country)] += 1

            total_processed += 1

        LOGGER.info(f"Rows processed: {len(results)} items")
        LOGGER.info(f"Total items processed so far: {total_processed}")
        LOGGER.info(f"Unique licenses: {len(license_counter)} | Languages: {len(language_counter)} | Countries: {len(country_counter)}")

    LOGGER.info("Finished processing.")
    return license_counter, language_counter, country_counter

def write_csv(file_path, header, rows):
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f, dialect="unix")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    LOGGER.info(f"Wrote {len(rows)} rows to {file_path}")


def write_all(args, license_counter, language_counter, country_counter):
    if not args.enable_save:
        return args

    os.makedirs(PATHS["data_phase"], exist_ok=True)

    write_csv(FILE1_COUNT, HEADER1, [(k[0], k[1], v) for k, v in license_counter.items()])
    write_csv(FILE2_LANGUAGE, HEADER2, [(k[0], k[1], k[2], v) for k, v in language_counter.items()])
    write_csv(FILE3_COUNTRY, HEADER3, [(k[0], k[1], k[2], v) for k, v in country_counter.items()])
    
    return args

def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)

    license_data, language_data, country_data = query_internet_archive(args)

    if args.enable_save:
        write_all(args, license_data, language_data, country_data)

    if args.enable_git:
        args = shared.git_add_and_commit(args, PATHS["repo"], PATHS["data_quarter"], f"Add Internet Archive data for {QUARTER}")
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
            highlight(traceback.format_exc(), PythonTracebackLexer(), TerminalFormatter()),
            "    ",
        )
        LOGGER.critical(f"(1) Unhandled exception:\n{traceback_formatted}")
        sys.exit(1)
