#!/usr/bin/env python
"""
Fetch CC Legal Tool usage data from Internet Archive (IA) API.
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
from copy import copy

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
BASE_URL = "https://archive.org/advancedsearch.php"
FILE1_COUNT = shared.path_join(PATHS["data_phase"], "ia_1_count.csv")
FILE2_LANGUAGE = shared.path_join(PATHS["data_phase"], "ia_2_count_by_language.csv")
FILE3_COUNTRY = shared.path_join(PATHS["data_phase"], "ia_3_count_by_country.csv")
HEADER1_COUNT = ["LICENSE_URL", "NORMALIZED_LICENSE", "COUNT"]
HEADER2_LANGUAGE = ["LICENSE_URL", "NORMALIZED_LICENSE", "LANGUAGE", "COUNT"]
HEADER3_COUNTRY = ["LICENSE_URL", "NORMALIZED_LICENSE", "COUNTRY", "COUNT"]
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
        default=1000,
        help="Limit items per query (default: 1000)",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=100000,
        help="Maximum total items to process (default: 100000)",
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


def get_requests_session():
    """
    Creates and returns a requests session with retry logic.
    """
    LOGGER.info("Setting up requests session with retry logic")
    max_retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update({
        "User-Agent": shared.USER_AGENT,
        "Accept": "application/json",
    })
    return session


def load_license_mapping():
    """
    Load the license mapping CSV file to normalize IA license URLs.
    """
    mapping_file = shared.path_join(PATHS["data"], "ia_license_mapping.csv")
    license_mapping = {}
    
    if os.path.exists(mapping_file):
        with open(mapping_file, "r", newline="", encoding="utf-8") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            for row in reader:
                license_mapping[row["IA_LICENSE_URL"]] = row["NORMALIZED_LICENSE"]
    else:
        LOGGER.warning(f"License mapping file not found: {mapping_file}")
        LOGGER.info("Creating default license mapping file")
        create_default_license_mapping(mapping_file)
        # Reload after creating
        with open(mapping_file, "r", newline="", encoding="utf-8") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            for row in reader:
                license_mapping[row["IA_LICENSE_URL"]] = row["NORMALIZED_LICENSE"]
    
    LOGGER.info(f"Loaded {len(license_mapping)} license mappings")
    return license_mapping


def create_default_license_mapping(mapping_file):
    """
    Create a default license mapping file with common CC license patterns.
    """
    os.makedirs(os.path.dirname(mapping_file), exist_ok=True)
    
    default_mappings = [
        # Creative Commons licenses
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by/4.0/", "NORMALIZED_LICENSE": "CC BY 4.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by/4.0/", "NORMALIZED_LICENSE": "CC BY 4.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-sa/4.0/", "NORMALIZED_LICENSE": "CC BY-SA 4.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-sa/4.0/", "NORMALIZED_LICENSE": "CC BY-SA 4.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nc/4.0/", "NORMALIZED_LICENSE": "CC BY-NC 4.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nc/4.0/", "NORMALIZED_LICENSE": "CC BY-NC 4.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nc-sa/4.0/", "NORMALIZED_LICENSE": "CC BY-NC-SA 4.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nc-sa/4.0/", "NORMALIZED_LICENSE": "CC BY-NC-SA 4.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nd/4.0/", "NORMALIZED_LICENSE": "CC BY-ND 4.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nd/4.0/", "NORMALIZED_LICENSE": "CC BY-ND 4.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nc-nd/4.0/", "NORMALIZED_LICENSE": "CC BY-NC-ND 4.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nc-nd/4.0/", "NORMALIZED_LICENSE": "CC BY-NC-ND 4.0"},
        
        # CC0 and Public Domain
        {"IA_LICENSE_URL": "http://creativecommons.org/publicdomain/zero/1.0/", "NORMALIZED_LICENSE": "CC0 1.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/publicdomain/zero/1.0/", "NORMALIZED_LICENSE": "CC0 1.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/publicdomain/mark/1.0/", "NORMALIZED_LICENSE": "PDM 1.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/publicdomain/mark/1.0/", "NORMALIZED_LICENSE": "PDM 1.0"},
        
        # Version 3.0 licenses
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by/3.0/", "NORMALIZED_LICENSE": "CC BY 3.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by/3.0/", "NORMALIZED_LICENSE": "CC BY 3.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-sa/3.0/", "NORMALIZED_LICENSE": "CC BY-SA 3.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-sa/3.0/", "NORMALIZED_LICENSE": "CC BY-SA 3.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nc/3.0/", "NORMALIZED_LICENSE": "CC BY-NC 3.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nc/3.0/", "NORMALIZED_LICENSE": "CC BY-NC 3.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nc-sa/3.0/", "NORMALIZED_LICENSE": "CC BY-NC-SA 3.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nc-sa/3.0/", "NORMALIZED_LICENSE": "CC BY-NC-SA 3.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nd/3.0/", "NORMALIZED_LICENSE": "CC BY-ND 3.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nd/3.0/", "NORMALIZED_LICENSE": "CC BY-ND 3.0"},
        {"IA_LICENSE_URL": "http://creativecommons.org/licenses/by-nc-nd/3.0/", "NORMALIZED_LICENSE": "CC BY-NC-ND 3.0"},
        {"IA_LICENSE_URL": "https://creativecommons.org/licenses/by-nc-nd/3.0/", "NORMALIZED_LICENSE": "CC BY-NC-ND 3.0"},
        
        # Other open licenses
        {"IA_LICENSE_URL": "http://www.gnu.org/licenses/gpl-3.0.html", "NORMALIZED_LICENSE": "GPL-3.0"},
        {"IA_LICENSE_URL": "https://www.gnu.org/licenses/gpl-3.0.html", "NORMALIZED_LICENSE": "GPL-3.0"},
        {"IA_LICENSE_URL": "http://www.gnu.org/licenses/agpl-3.0.html", "NORMALIZED_LICENSE": "AGPL-3.0"},
        {"IA_LICENSE_URL": "https://www.gnu.org/licenses/agpl-3.0.html", "NORMALIZED_LICENSE": "AGPL-3.0"},
        {"IA_LICENSE_URL": "http://www.gnu.org/licenses/lgpl-3.0.html", "NORMALIZED_LICENSE": "LGPL-3.0"},
        {"IA_LICENSE_URL": "https://www.gnu.org/licenses/lgpl-3.0.html", "NORMALIZED_LICENSE": "LGPL-3.0"},
        {"IA_LICENSE_URL": "http://opensource.org/licenses/MIT", "NORMALIZED_LICENSE": "MIT"},
        {"IA_LICENSE_URL": "https://opensource.org/licenses/MIT", "NORMALIZED_LICENSE": "MIT"},
        {"IA_LICENSE_URL": "http://opensource.org/licenses/Apache-2.0", "NORMALIZED_LICENSE": "Apache-2.0"},
        {"IA_LICENSE_URL": "https://opensource.org/licenses/Apache-2.0", "NORMALIZED_LICENSE": "Apache-2.0"},
        {"IA_LICENSE_URL": "http://opensource.org/licenses/BSD-3-Clause", "NORMALIZED_LICENSE": "BSD-3-Clause"},
        {"IA_LICENSE_URL": "https://opensource.org/licenses/BSD-3-Clause", "NORMALIZED_LICENSE": "BSD-3-Clause"},
    ]
    
    with open(mapping_file, "w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["IA_LICENSE_URL", "NORMALIZED_LICENSE"], dialect="unix")
        writer.writeheader()
        writer.writerows(default_mappings)
    
    LOGGER.info(f"Created default license mapping file: {mapping_file}")


def initialize_data_files(args):
    """
    Initialize all data files for IA data collection.
    """
    if not args.enable_save:
        return

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    # Initialize count file
    if not os.path.isfile(FILE1_COUNT):
        with open(FILE1_COUNT, "w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=HEADER1_COUNT, dialect="unix")
            writer.writeheader()

    # Initialize language file
    if not os.path.isfile(FILE2_LANGUAGE):
        with open(FILE2_LANGUAGE, "w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=HEADER2_LANGUAGE, dialect="unix")
            writer.writeheader()

    # Initialize country file
    if not os.path.isfile(FILE3_COUNTRY):
        with open(FILE3_COUNTRY, "w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=HEADER3_COUNTRY, dialect="unix")
            writer.writeheader()


def query_ia_api(args, session, license_mapping, offset=0):
    """
    Query the Internet Archive API for CC-licensed items.
    """
    LOGGER.info(f"Querying IA API with offset {offset}")
    
    # Build search query for Creative Commons licenses
    query_params = {
        "q": "licenseurl:creativecommons.org OR licenseurl:gnu.org OR licenseurl:opensource.org",
        "fl": "identifier,licenseurl,language,country,mediatype",
        "rows": args.limit,
        "start": offset,
        "output": "json",
        "sort": "identifier asc"
    }
    
    if args.dev:
        # Generate fake data for development
        LOGGER.info("Development mode: generating fake data")
        return generate_fake_data(license_mapping, args.limit)
    
    try:
        response = session.get(BASE_URL, params=query_params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if "response" not in data:
            LOGGER.error(f"Unexpected API response structure: {data}")
            return []
        
        docs = data["response"].get("docs", [])
        total_found = data["response"].get("numFound", 0)
        
        LOGGER.info(f"Retrieved {len(docs)} items (total found: {total_found})")
        return docs, total_found
        
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Error querying IA API: {e}")
        return [], 0


def generate_fake_data(license_mapping, count):
    """
    Generate fake data for development/testing purposes.
    """
    import random
    
    fake_licenses = list(license_mapping.values())[:10]  # Use first 10 licenses
    fake_languages = ["en", "es", "fr", "de", "it", "pt", "ru", "zh", "ja", "ar"]
    fake_countries = ["US", "GB", "CA", "AU", "DE", "FR", "IT", "ES", "BR", "MX"]
    
    docs = []
    for i in range(count):
        docs.append({
            "identifier": f"fake_item_{i:06d}",
            "licenseurl": random.choice(list(license_mapping.keys())),
            "language": random.choice(fake_languages),
            "country": random.choice(fake_countries),
            "mediatype": random.choice(["texts", "audio", "video", "image", "software"])
        })
    
    return docs, count


def normalize_license(license_url, license_mapping):
    """
    Normalize a license URL using the mapping table.
    """
    if not license_url:
        return "Unknown"
    
    # Direct lookup
    if license_url in license_mapping:
        return license_mapping[license_url]
    
    # Try to match partial URLs
    for ia_url, normalized in license_mapping.items():
        if ia_url in license_url or license_url in ia_url:
            return normalized
    
    # If no match found, return a cleaned version of the URL
    return license_url.split("/")[-2] if "/" in license_url else license_url


def process_ia_data(args, docs, license_mapping):
    """
    Process IA API results and aggregate data.
    """
    LOGGER.info(f"Processing {len(docs)} IA items")
    
    # Initialize counters
    license_counts = {}
    language_counts = {}
    country_counts = {}
    
    for doc in docs:
        license_url = doc.get("licenseurl", "")
        normalized_license = normalize_license(license_url, license_mapping)
        language = doc.get("language", "Unknown")
        country = doc.get("country", "Unknown")
        
        # Count by license
        key = (license_url, normalized_license)
        license_counts[key] = license_counts.get(key, 0) + 1
        
        # Count by language
        lang_key = (license_url, normalized_license, language)
        language_counts[lang_key] = language_counts.get(lang_key, 0) + 1
        
        # Count by country
        country_key = (license_url, normalized_license, country)
        country_counts[country_key] = country_counts.get(country_key, 0) + 1
    
    # Write data to files
    write_license_data(args, license_counts)
    write_language_data(args, language_counts)
    write_country_data(args, country_counts)
    
    return len(docs)


def write_license_data(args, license_counts):
    """
    Write license count data to CSV file.
    """
    if not args.enable_save:
        return
    
    with open(FILE1_COUNT, "a", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=HEADER1_COUNT, dialect="unix")
        for (license_url, normalized_license), count in license_counts.items():
            writer.writerow({
                "LICENSE_URL": license_url,
                "NORMALIZED_LICENSE": normalized_license,
                "COUNT": count
            })


def write_language_data(args, language_counts):
    """
    Write language count data to CSV file.
    """
    if not args.enable_save:
        return
    
    with open(FILE2_LANGUAGE, "a", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=HEADER2_LANGUAGE, dialect="unix")
        for (license_url, normalized_license, language), count in language_counts.items():
            writer.writerow({
                "LICENSE_URL": license_url,
                "NORMALIZED_LICENSE": normalized_license,
                "LANGUAGE": language,
                "COUNT": count
            })


def write_country_data(args, country_counts):
    """
    Write country count data to CSV file.
    """
    if not args.enable_save:
        return
    
    with open(FILE3_COUNTRY, "a", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=HEADER3_COUNTRY, dialect="unix")
        for (license_url, normalized_license, country), count in country_counts.items():
            writer.writerow({
                "LICENSE_URL": license_url,
                "NORMALIZED_LICENSE": normalized_license,
                "COUNTRY": country,
                "COUNT": count
            })


def main():
    """
    Main function to orchestrate IA data collection.
    """
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    
    # Load license mapping
    license_mapping = load_license_mapping()
    
    # Initialize data files
    initialize_data_files(args)
    
    # Setup session
    session = get_requests_session()
    
    # Fetch and process data
    total_processed = 0
    offset = 0
    
    while total_processed < args.max_items:
        remaining = args.max_items - total_processed
        current_limit = min(args.limit, remaining)
        
        LOGGER.info(f"Processing batch: offset={offset}, limit={current_limit}")
        
        if args.dev:
            docs, total_found = query_ia_api(args, session, license_mapping, offset)
        else:
            docs, total_found = query_ia_api(args, session, license_mapping, offset)
        
        if not docs:
            LOGGER.info("No more data available")
            break
        
        # Process the batch
        batch_processed = process_ia_data(args, docs, license_mapping)
        total_processed += batch_processed
        
        LOGGER.info(f"Processed {batch_processed} items (total: {total_processed})")
        
        # Check if we've reached the end
        if len(docs) < current_limit:
            LOGGER.info("Reached end of available data")
            break
        
        # Update offset for next batch
        offset += len(docs)
        
        # Rate limiting
        if not args.dev:
            time.sleep(1)  # Be respectful to the API
    
    LOGGER.info(f"Data collection completed. Total items processed: {total_processed}")
    
    # Git operations
    shared.git_fetch_and_merge(args, PATHS["repo"])
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Internet Archive (IA) data for {QUARTER}",
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

