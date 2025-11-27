#!/usr/bin/env python
"""
Fetch Zenodo records with license information using REST API and generate
count reports.

This implementation uses Zenodo's REST API instead of OAI-PMH for more reliable
license detection. Benefits include:
- Structured license data (metadata.license.id)
- Clear separation of access rights
- JSON parsing
- Standardized license identifiers
"""
# Standard library
import argparse
import csv
import json
import os
import sys
import textwrap
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime

# Third-party
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
# Implementation choice: set to capture comprehensive CC license data
DEFAULT_FETCH_LIMIT = (
    100000  # Increased to capture more CC licenses from 5.5M+ records
)
# API limitation: Zenodo supports 1000+ records per request, 300 chosen
MAX_RECORDS_PER_REQUEST = 300
ZENODO_API_BASE_URL = "https://zenodo.org/api/records"

# CSV Headers
HEADER_COUNT = ["LICENSE_TYPE", "COUNT"]
HEADER_LANGUAGE = ["LICENSE_TYPE", "LANGUAGE", "COUNT"]
HEADER_TYPE = ["LICENSE_TYPE", "RESOURCE_TYPE", "COUNT"]
HEADER_YEAR = ["LICENSE_TYPE", "YEAR", "COUNT"]

# CC License mapping for Zenodo API
cc_license_mapping = {
    "cc-by": "CC BY 4.0",
    "cc-by-sa": "CC BY-SA 4.0",
    "cc-by-nc": "CC BY-NC 4.0",
    "cc-by-nd": "CC BY-ND 4.0",
    "cc-by-nc-sa": "CC BY-NC-SA 4.0",
    "cc-by-nc-nd": "CC BY-NC-ND 4.0",
}


def parse_arguments():
    """Parse command-line options, returns parsed argument namespace."""
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=os.path.basename(PATHS["data_quarter"]),
        help="Data quarter in format YYYYQx",
    )
    parser.add_argument(
        "--fetch-limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=f"Total records to fetch (default: {DEFAULT_FETCH_LIMIT})",
    )
    parser.add_argument(
        "--dates-back",
        type=int,
        help="Fetch records from N years back to present "
        "(e.g., --dates-back 5 for last 5 years)",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results (default: False)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (default: False)",
    )
    return parser.parse_args()


def setup_session():
    """Setup requests session with retry strategy."""
    session = requests.Session()
    session.headers.update({"User-Agent": shared.USER_AGENT})
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


def classify_resource_type(type_text):
    """Classify resource type using Zenodo's detailed taxonomy with names."""
    if not type_text:
        return "Unknown"

    text = type_text.lower()

    # Alphabetical order by return value
    if (
        "publication-annotationcollection" in text
        or "annotation collection" in text
    ):
        return "Annotation Collection"
    elif "publication-book" in text or "book" in text:
        return "Book"
    elif "publication-section" in text or "book chapter" in text:
        return "Book Chapter"
    elif (
        "software-computationalnotebook" in text
        or "computational notebook" in text
    ):
        return "Computational Notebook"
    elif "publication-conferencepaper" in text or "conference paper" in text:
        return "Conference Paper"
    elif (
        "publication-conferenceproceeding" in text
        or "conference proceeding" in text
    ):
        return "Conference Proceeding"
    elif "publication-datapaper" in text or "data paper" in text:
        return "Data Paper"
    elif "dataset" in text:
        return "Dataset"
    elif "image-diagram" in text or "diagram" in text:
        return "Diagram"
    elif "publication-dissertation" in text or "dissertation" in text:
        return "Dissertation"
    elif "image-drawing" in text or "drawing" in text:
        return "Drawing"
    elif "event" in text:
        return "Event"
    elif "image-figure" in text or "figure" in text:
        return "Figure"
    elif "image" in text:
        return "Image"
    elif "publication-journal" in text or "journal" in text:
        return "Journal"
    elif "publication-article" in text or "journal article" in text:
        return "Journal Article"
    elif "lesson" in text:
        return "Lesson"
    elif "model" in text:
        return "Model"
    elif "other" in text:
        return "Other"
    elif "image-other" in text or ("image" in text and "other" in text):
        return "Other Image"
    elif "publication-other" in text or (
        "publication" in text and "other" in text
    ):
        return "Other Publication"
    elif (
        "publication-datamanagementplan" in text
        or "output management plan" in text
    ):
        return "Output Management Plan"
    elif "publication-patent" in text or "patent" in text:
        return "Patent"
    elif "publication-peerreview" in text or "peer review" in text:
        return "Peer Review"
    elif "image-photo" in text or "photo" in text:
        return "Photo"
    elif "image-plot" in text or "plot" in text:
        return "Plot"
    elif "poster" in text:
        return "Poster"
    elif "presentation" in text or "lecture" in text:
        return "Presentation"
    elif "publication-preprint" in text or "preprint" in text:
        return "Preprint"
    elif "publication-deliverable" in text or "project deliverable" in text:
        return "Project Deliverable"
    elif "publication" in text or "article" in text:
        return "Publication"
    elif "publication-report" in text or "report" in text:
        return "Report"
    elif "software" in text or "code" in text:
        return "Software"
    elif (
        "publication-taxonomictreatment" in text
        or "taxonomic treatment" in text
    ):
        return "Taxonomic Treatment"
    elif "video" in text or "audio" in text:
        return "Video/Audio"

    return "Unclassified"


def standardize_language(lang_text):
    """Standardize language codes/names using pycountry."""
    if not lang_text:
        return "Unknown"

    # First-party/Local
    from shared import get_language_name

    return get_language_name(lang_text.strip())


def classify_license(license_data):
    """Classify license from Zenodo REST API, focusing on CC licenses."""
    if not license_data:
        return "No License"

    # REST API provides structured license data
    if isinstance(license_data, dict):
        license_id = license_data.get("id", "")  # API returns lowercase IDs

        # Focus on Creative Commons licenses - normalize most cases
        if license_id.startswith("cc"):
            # Special cases that need custom mapping
            special_cases = {
                "cc-zero": "CC0",
                "cc0-1.0": "CC0",
                "cc0": "CC0",
                "cc-nc": "CC BY-NC 4.0",
                "cc-nd": "CC BY-ND 4.0",
                "cc-sa": "CC BY-SA 4.0",
                "cc-publicdomain": "Public Domain Mark 1.0",
            }

            if license_id in special_cases:
                return special_cases[license_id]

            # Normalize standard CC licenses: cc-by-4.0 -> CC BY 4.0
            if license_id.startswith("cc-by"):
                return license_id.upper().replace("-", " ")

        # Check if it's a CC license
        if license_id in cc_license_mapping:
            return cc_license_mapping[license_id]

        # Group all non-CC licenses as "Non-CC" for efficiency
        if license_id and license_id not in ["notspecified", ""]:
            return "Non-CC"

        return "Not Specified"

    return "Unknown"


def fetch_zenodo_records(session, page=1, size=100, query="*"):
    """
    Fetch Zenodo records using REST API.

    Returns JSON response containing structured record data with:
    - metadata.license (structured license object)
    - metadata.resource_type (structured type information)
    """
    params = {
        "q": query,
        "size": min(size, MAX_RECORDS_PER_REQUEST),
        "page": page,
        "sort": "bestmatch",
    }

    try:
        response = session.get(ZENODO_API_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise shared.QuantifyingException(
            f"Error fetching Zenodo records: {e}"
        )
    except json.JSONDecodeError as e:
        raise shared.QuantifyingException(f"Error parsing JSON response: {e}")


def extract_record_info(record_json):
    """
    Extract information from a Zenodo REST API record.

    Parses JSON structure:
    - metadata.license (structured license object)
    - metadata.resource_type (structured type information)
    - metadata.publication_date (ISO date)
    - metadata.language (language code)
    """
    record_info = {
        "identifier": None,
        "title": None,
        "date": None,
        "type": None,
        "language": None,
        "license_type": None,
    }

    try:
        # Basic record info
        record_info["identifier"] = record_json.get("id")

        metadata = record_json.get("metadata", {})

        # Title
        record_info["title"] = metadata.get("title")

        # Date - extract year from publication_date
        pub_date = metadata.get("publication_date")
        if pub_date:
            try:
                record_info["date"] = int(pub_date[:4])
            except (ValueError, TypeError) as e:
                LOGGER.debug(f"Invalid date format '{pub_date}': {e}")
                record_info["date"] = None

        # Resource type
        resource_type = metadata.get("resource_type", {})
        if isinstance(resource_type, dict):
            type_title = resource_type.get("title", "")
            record_info["type"] = classify_resource_type(type_title)

        # Language
        language = metadata.get("language")
        if language:
            record_info["language"] = standardize_language(language)

        # License - structured data from REST API
        license_data = metadata.get("license")
        if license_data:
            license_type = classify_license(license_data)
            if license_type:
                record_info["license_type"] = license_type
                LOGGER.debug(
                    f"License identified: '{license_type}' from API data: "
                    f"{license_data}"
                )

    except Exception as e:
        LOGGER.warning(
            f"Error extracting record info from record "
            f"{record_json.get('id', 'unknown')}: {e}"
        )
        return record_info

    return record_info


def save_csv(file_path, headers, data, args):
    """Save data to CSV file if save is enabled."""
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", newline="\n", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(headers)
            writer.writerows(data)


def main():
    args = parse_arguments()

    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    session = setup_session()
    all_records = []
    extraction_failures = 0
    total_processed = 0
    license_counts = Counter()
    year_counts = defaultdict(Counter)
    type_counts = defaultdict(Counter)
    lang_counts = defaultdict(Counter)

    # Build query for all records - CC filtering happens during processing
    # Note: Zenodo's search API doesn't support reliable license field queries
    base_query = "*"

    if args.dates_back:
        from_year = datetime.now().year - args.dates_back
        query = f"publication_date:[{from_year}-01-01 TO *]"
        LOGGER.info(f"Filtering records from {from_year} onwards")
    else:
        query = base_query
        LOGGER.info(
            "Fetching all records, filtering CC licenses during processing"
        )

    # Fetch Zenodo records using REST API
    LOGGER.info("Fetching Zenodo records using REST API")

    page = 1
    total_fetched = 0
    records_per_page = min(MAX_RECORDS_PER_REQUEST, 100)

    while total_fetched < args.fetch_limit:
        LOGGER.info(f"Fetching page {page} (total so far: {total_fetched})")

        try:
            response_data = fetch_zenodo_records(
                session, page=page, size=records_per_page, query=query
            )
        except (requests.RequestException, json.JSONDecodeError) as e:
            raise shared.QuantifyingException(
                f"Failed to fetch Zenodo records: {e}"
            )

        if not response_data or "hits" not in response_data:
            raise shared.QuantifyingException(
                "Invalid response from Zenodo REST API - stopping execution"
            )

        hits = response_data["hits"]
        records = hits.get("hits", [])
        total_available = hits.get("total", 0)

        LOGGER.info(f"Total records available: {total_available}")

        if not records:
            LOGGER.info("No more records available - completed fetching")
            break

        batch_license_records_found = 0

        for record in records:
            if total_fetched >= args.fetch_limit:
                break

            record_info = extract_record_info(record)
            total_processed += 1

            # Skip records where extraction failed
            if not record_info:
                extraction_failures += 1
                continue

            # Only include records with valid licenses (CC filtering at API)
            if record_info.get("license_type") and record_info[
                "license_type"
            ] not in ["Unknown", "No License"]:
                # Apply additional date filtering if specified
                if args.dates_back and record_info["date"]:
                    publication_year_cutoff = (
                        datetime.now().year - args.dates_back
                    )
                    if record_info["date"] < publication_year_cutoff:
                        continue

                all_records.append(record_info)
                batch_license_records_found += 1
                total_fetched += 1

        # Check if we've reached the end of available records
        if len(records) < records_per_page:
            LOGGER.info("Reached end of available records")
            break

        page += 1

        # Be respectful to the API - increased delay for rate limiting
        time.sleep(2.0)

    # Check for excessive extraction failures
    if total_processed > 0:
        failure_rate = extraction_failures / total_processed
        if failure_rate > 0.1:  # More than 10% failures
            raise shared.QuantifyingException(
                f"Too many extraction failures: {extraction_failures}/"
                f"{total_processed} ({failure_rate:.1%}) - data quality issues"
            )
        elif extraction_failures > 0:
            LOGGER.warning(
                f"Extraction failures: {extraction_failures}/"
                f"{total_processed} ({failure_rate:.1%})"
            )

    if not all_records:
        LOGGER.warning("No CC-licensed records found")
    else:
        LOGGER.info(f"Found {len(all_records)} total CC-licensed records")

    # Generate counts and log summary
    total_license_records = 0
    for record in all_records:
        license_type = record["license_type"]
        year = record["date"]
        resource_type = record["type"]
        language = record["language"]

        # Only count records with actual licenses (skip None/null license_type)
        if license_type:
            license_counts[license_type] += 1
            total_license_records += 1

            if year:
                year_counts[license_type][year] += 1

            if resource_type:
                type_counts[license_type][resource_type] += 1

            if language:
                lang_counts[license_type][language] += 1

    # Log summary
    LOGGER.info(
        f"Processed {total_license_records} records with valid licenses"
    )
    LOGGER.info("License distribution:")
    for license_type, count in sorted(license_counts.items()):
        LOGGER.info(f"  {license_type}: {count}")

    # Save results
    # 1. License totals
    license_data = [
        [license_type, count]
        for license_type, count in sorted(license_counts.items())
    ]
    save_csv(
        shared.path_join(PATHS["data_phase"], "zenodo_1_count.csv"),
        HEADER_COUNT,
        license_data,
        args,
    )

    # 2. Year breakdown
    year_data = []
    for license_type in sorted(year_counts.keys()):
        for year, count in sorted(year_counts[license_type].items()):
            year_data.append([license_type, year, count])
    save_csv(
        shared.path_join(PATHS["data_phase"], "zenodo_2_count_by_year.csv"),
        HEADER_YEAR,
        year_data,
        args,
    )

    # 3. Resource type breakdown
    type_data = []
    for license_type in sorted(type_counts.keys()):
        for resource_type, count in sorted(type_counts[license_type].items()):
            type_data.append([license_type, resource_type, count])
    save_csv(
        shared.path_join(PATHS["data_phase"], "zenodo_3_count_by_type.csv"),
        HEADER_TYPE,
        type_data,
        args,
    )

    # 4. Language breakdown
    lang_data = []
    for license_type in sorted(lang_counts.keys()):
        for language, count in sorted(lang_counts[license_type].items()):
            lang_data.append([license_type, language, count])
    save_csv(
        shared.path_join(
            PATHS["data_phase"], "zenodo_4_count_by_language.csv"
        ),
        HEADER_LANGUAGE,
        lang_data,
        args,
    )

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Zenodo data for {args.quarter}",
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
