#!/usr/bin/env python
"""
Fetch open content data from Internet Archive using the Python interface.
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
import pycountry
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# First-party/Local
from internetarchive import search_items

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
FILE1_COUNT = os.path.join(PATHS["data_phase"], "internetarchive_1_count.csv")
FILE2_LANGUAGE = os.path.join(
    PATHS["data_phase"], "internetarchive_2_count_by_language.csv"
)
FILE3_PORT = os.path.join(
    PATHS["data_phase"], "internetarchive_3_count_by_port.csv"
)

HEADER1 = ["LICENSE_URL", "LICENSE", "COUNT"]
HEADER2 = ["LICENSE_URL", "LICENSE", "LANGUAGE", "COUNT"]
HEADER3 = ["LICENSE_URL", "LICENSE", "PORT", "COUNT"]

QUARTER = os.path.basename(PATHS["data_quarter"])


def parse_arguments():
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enable-save", action="store_true", help="Enable saving results"
    )
    parser.add_argument(
        "--enable-git", action="store_true", help="Enable git actions"
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def load_license_mapping(port_mapping):
    """Loads and normalizes the license mapping from CSV."""
    license_mapping = {}
    file_path = shared.path_join(
        PATHS["data"], "license_url_to_identifier_mapping.csv"
    )
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_url = row["LICENSE_URL"]
            label = row["LICENSE"].strip()
            normalized_url, _ = normalize_license(
                raw_url, license_mapping=None, port_mapping=port_mapping
            )
            if normalized_url:
                license_mapping[normalized_url] = label
    return license_mapping


def load_port_mapping():
    """Maps jurisdiction codes to country names using pycountry."""
    port_mapping = {}
    for country in pycountry.countries:
        code = country.alpha_2.lower()
        port_mapping[code] = country.name
    return port_mapping


def normalize_license(licenseurl, license_mapping=None, port_mapping=None):
    """Normalize licenseurl to match TOOL_URL format"""
    """map to TOOL_IDENTIFIER and jurisdiction."""
    if not isinstance(licenseurl, str) or not licenseurl.strip():
        return None, None

    # Cleanup
    normalized = (
        licenseurl.lower()
        .strip()
        .replace("http://", "https://")
        .replace("www.", "")
        .rstrip("/")
    )

    # Remove /legalcode or /deed only if they are suffixes
    for suffix in ["/legalcode", "/deed"]:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]

    # Ensure leading double slashes to match TOOL_URL format
    if not normalized.startswith("https://"):
        normalized = "https://" + normalized

    # Extract jurisdiction code from final segment
    parts = normalized.split("/")
    jurisdiction = None
    if len(parts) >= 6 and parts[-1].isalpha() and len(parts[-1]) <= 4:
        jurisdiction = parts[-1].lower()

    # Lookup LICENSE_URL
    label = (
        license_mapping.get(normalized, "UNKNOWN")
        if license_mapping
        else normalized
    )

    return label, jurisdiction


def query_internet_archive(args):
    license_counter = Counter()
    language_counter = Counter()
    port_counter = Counter()

    fields = ["licenseurl", "language"]
    query = "creativecommons.org"
    port_mapping = load_port_mapping()
    license_mapping = load_license_mapping(port_mapping)

    rows = 100000
    total_rows = 0
    total_processed = 0
    max_retries = 3

    while True:
        # Loop until no more results are returned by the API
        LOGGER.info(f"Fetching {rows} items...")
        results = None
        for attempt in range(max_retries):
            try:
                # Use search_items for simpler pagination management
                search = search_items(
                    query,
                    fields=fields,
                    params={"rows": rows, "start": total_rows},
                    request_kwargs={"timeout": 120},
                )

                # Convert to list to iterate over
                results = list(search)
                total_rows += len(results)
                break
            except Exception as e:
                wait_time = 2**attempt
                # Fixed
                LOGGER.warning(
                    f"API request failed (Attempt {attempt+1}/{max_retries}). "
                    f"Waiting {wait_time}s. Error: {e}"
                )
                sleep(wait_time)
        else:
            LOGGER.error(
                f"Failed to fetch data after {max_retries} attempts."
                "Stopping script."
            )
            break

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

            normalized_url, jurisdiction = normalize_license(
                licenseurl, license_mapping, port_mapping
            )

            # Use jurisdiction as port country
            port = jurisdiction

            if normalized_url == "UNKNOWN":
                LOGGER.warning(
                    f"Unmapped normalized URL: {licenseurl} → {normalized_url}"
                )

            license_counter[(licenseurl, normalized_url)] += 1
            language_counter[(licenseurl, normalized_url, language)] += 1
            port_counter[(licenseurl, normalized_url, port)] += 1

            total_processed += 1

        LOGGER.info(f"Rows processed: {len(results)} items")
        LOGGER.info(f"Total items processed so far: {total_processed}")
        LOGGER.info(
            f"Unique licenses: {len(license_counter)}|"
            f"Languages:{len(language_counter)}|"
            f"Countries:{len(port_counter)}"
        )

        # If the results is less than the requested rows, implies the end
        if len(results) < rows:
            LOGGER.info(
                "Last chunk was smaller than requested rows."
                "Ending pagination."
            )
            break

    LOGGER.info("Finished processing.")
    return license_counter, language_counter, port_counter


def write_csv(file_path, header, rows):
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f, dialect="unix")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    LOGGER.info(f"Wrote {len(rows)} rows to {file_path}")


def write_all(args, license_counter, language_counter, port_counter):
    if not args.enable_save:
        return args

    os.makedirs(PATHS["data_phase"], exist_ok=True)

    write_csv(
        FILE1_COUNT,
        HEADER1,
        [(k[0], k[1], v) for k, v in license_counter.items()],
    )
    write_csv(
        FILE2_LANGUAGE,
        HEADER2,
        [(k[0], k[1], k[2], v) for k, v in language_counter.items()],
    )
    write_csv(
        FILE3_PORT,
        HEADER3,
        [(k[0], k[1], k[2], v) for k, v in port_counter.items()],
    )

    return args


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)

    license_data, language_data, port_data = query_internet_archive(args)

    if args.enable_save:
        write_all(args, license_data, language_data, port_data)

    if args.enable_git:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add Internet Archive data for {QUARTER}",
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
