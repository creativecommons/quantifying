#!/usr/bin/env python
"""
Fetch Europeana cultural heritage data with Creative Commons license info.

This script queries the Europeana API to collect statistics on openly licensed
cultural heritage items, providing data for quantifying commons adoption
across European institutions.
"""
# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback
from collections import defaultdict

# Third-party
import requests
from dotenv import load_dotenv
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

# API Configuration
BASE_URL = "https://api.europeana.eu/record/v2/search.json"
EUROPEANA_KEY = os.getenv("EUROPEANA_KEY")

# Rate limiting configuration (conservative for API stability)
MAX_RETRIES = 3
INITIAL_DELAY = 0.5  # seconds
RATE_DELAY = 0.3  # seconds between requests
REQUEST_TIMEOUT = 30  # seconds
BATCH_SIZE = 100  # items per request (API maximum)

# Output file paths
FILE_EUROPEANA_COUNT = shared.path_join(
    PATHS["data_phase"], "europeana_1_count.csv"
)
FILE_EUROPEANA_COUNTRY = shared.path_join(
    PATHS["data_phase"], "europeana_2_count_by_country.csv"
)
FILE_EUROPEANA_TYPE = shared.path_join(
    PATHS["data_phase"], "europeana_3_count_by_type.csv"
)
FILE_EUROPEANA_YEAR = shared.path_join(
    PATHS["data_phase"], "europeana_4_count_by_year.csv"
)
FILE_EUROPEANA_PROVIDER = shared.path_join(
    PATHS["data_phase"], "europeana_5_count_by_provider.csv"
)

# CSV headers
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_COUNTRY = ["TOOL_IDENTIFIER", "COUNTRY", "COUNT"]
HEADER_TYPE = ["TOOL_IDENTIFIER", "TYPE", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]
HEADER_PROVIDER = ["DATA_PROVIDER", "LEGAL_TOOL", "COUNT"]

QUARTER = os.path.basename(PATHS["data_quarter"])

# Log the start of the script execution
LOGGER.info("Script execution started.")


def parse_arguments():
    """
    Parse command-line options and validate arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Limit number of items to fetch (default: 1000)",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results to CSV files",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, and push)",
    )
    args = parser.parse_args()

    # Validate argument dependencies
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")

    # Validate API key availability
    if not EUROPEANA_KEY:
        parser.error("EUROPEANA_KEY environment variable is required")

    return args


def initialize_data_file(file_path, headers):
    """
    Initialize CSV file with headers if it doesn't exist.

    Args:
        file_path (str): Path to the CSV file
        headers (list): List of column headers
    """
    if not os.path.isfile(file_path):
        with open(file_path, "w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=headers, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """
    Initialize all output CSV files with appropriate headers.

    Args:
        args (argparse.Namespace): Command-line arguments
    """
    if not args.enable_save:
        return

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    # Initialize all CSV files
    initialize_data_file(FILE_EUROPEANA_COUNT, HEADER_COUNT)
    initialize_data_file(FILE_EUROPEANA_COUNTRY, HEADER_COUNTRY)
    initialize_data_file(FILE_EUROPEANA_TYPE, HEADER_TYPE)
    initialize_data_file(FILE_EUROPEANA_YEAR, HEADER_YEAR)
    initialize_data_file(FILE_EUROPEANA_PROVIDER, HEADER_PROVIDER)


def extract_license_from_rights(rights_list):
    """
    Extract Creative Commons license type from rights field.

    Args:
        rights_list (list or str): Rights information from API response

    Returns:
        str: Standardized license identifier
    """
    if not rights_list:
        return "Unknown"

    # Handle both list and string formats
    rights_url = (
        rights_list[0] if isinstance(rights_list, list) else rights_list
    )
    rights_lower = rights_url.lower()

    # Map rights URLs to standardized license identifiers
    if (
        "creativecommons.org/publicdomain/zero" in rights_lower
        or "/cc0/" in rights_lower
    ):
        return "CC0"
    elif "creativecommons.org/licenses/by-nc-nd" in rights_lower:
        return "CC BY-NC-ND"
    elif "creativecommons.org/licenses/by-nc-sa" in rights_lower:
        return "CC BY-NC-SA"
    elif "creativecommons.org/licenses/by-nd" in rights_lower:
        return "CC BY-ND"
    elif "creativecommons.org/licenses/by-sa" in rights_lower:
        return "CC BY-SA"
    elif "creativecommons.org/licenses/by-nc" in rights_lower:
        return "CC BY-NC"
    elif "creativecommons.org/licenses/by" in rights_lower:
        return "CC BY"
    elif "creativecommons.org" in rights_lower:
        return "Creative Commons"

    return "Unknown"


def extract_year_from_item(item):
    """
    Extract publication/creation year from item metadata.

    Args:
        item (dict): Item metadata from API response

    Returns:
        str: Year as string, or "Unknown" if not available
    """
    # Try year field first (most reliable)
    if "year" in item and item["year"]:
        years = item["year"]
        if isinstance(years, list) and years:
            return str(years[0])
        elif isinstance(years, str):
            return years

    # Try timestamp_created as fallback
    if "timestamp_created" in item and item["timestamp_created"]:
        timestamp = item["timestamp_created"]
        try:
            return timestamp[:4]  # Extract year from ISO date
        except (TypeError, IndexError):
            pass

    return "Unknown"


def make_api_request(params):
    """
    Make a single API request with proper error handling and rate limiting.

    Args:
        params (dict): API request parameters

    Returns:
        dict or None: API response data, or None if request failed

    Raises:
        requests.exceptions.RequestException: For quota/rate limit issues
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                BASE_URL, params=params, timeout=REQUEST_TIMEOUT
            )

            # Handle quota/rate limit responses
            if response.status_code == 429:
                LOGGER.warning("Rate limit exceeded, stopping execution")
                raise requests.exceptions.RequestException(
                    "Rate limit exceeded"
                )

            response.raise_for_status()
            data = response.json()

            # Check API-level success
            if not data.get("success", False):
                error_msg = data.get("error", "Unknown API error")
                if any(
                    keyword in error_msg.lower()
                    for keyword in ["quota", "limit"]
                ):
                    LOGGER.warning(f"API quota exceeded: {error_msg}")
                    raise requests.exceptions.RequestException(
                        "Quota exceeded"
                    )
                LOGGER.warning(f"API returned success=false: {data}")
                return None

            return data

        except requests.exceptions.Timeout:
            LOGGER.warning(f"Request timeout on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            if "quota" in str(e).lower() or "limit" in str(e).lower():
                raise  # Re-raise quota/limit errors to stop execution
            LOGGER.warning(f"Request failed on attempt {attempt + 1}: {e}")

        # Exponential backoff for retries
        if attempt < MAX_RETRIES - 1:
            delay = INITIAL_DELAY * (2**attempt)
            LOGGER.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    LOGGER.error("Max retries exceeded for API request")
    return None


def query_europeana(args):
    """
    Query Europeana API for Creative Commons licensed cultural heritage items.

    Args:
        args (argparse.Namespace): Command-line arguments

    Returns:
        None
    """
    LOGGER.info("Beginning to fetch results from Europeana API")

    # Search queries for different reusability types
    reusability_queries = [
        ("open", "*"),
        ("restricted", "*"),
        ("permission", "*"),
    ]

    # Data structures for counting
    license_counts = defaultdict(int)
    country_counts = defaultdict(lambda: defaultdict(int))
    type_counts = defaultdict(lambda: defaultdict(int))
    year_counts = defaultdict(lambda: defaultdict(int))
    provider_counts = defaultdict(lambda: defaultdict(int))

    total_fetched = 0

    try:
        for reusability_type, query in reusability_queries:
            if total_fetched >= args.limit:
                break

            LOGGER.info(f"Searching for reusability: {reusability_type}")
            consecutive_empty_calls = 0

            # Process data in batches
            for start in range(
                0, min(args.limit - total_fetched, 1000), BATCH_SIZE
            ):
                params = {
                    "wskey": EUROPEANA_KEY,
                    "query": query,
                    "reusability": reusability_type,
                    "start": start + 1,  # Europeana uses 1-based indexing
                    "rows": BATCH_SIZE,
                }

                LOGGER.info(
                    f"Fetching results {start + 1} - {start + BATCH_SIZE}"
                )

                # Make API request with error handling
                data = make_api_request(params)
                if data is None:
                    LOGGER.error("Failed to fetch data, skipping batch")
                    break

                items = data.get("items", [])
                items_found_in_batch = 0

                # Process each item in the batch
                for item in items:
                    if total_fetched >= args.limit:
                        break

                    # Extract license information
                    rights = item.get("rights", [])
                    license_type = extract_license_from_rights(rights)

                    # Use reusability type as primary identifier
                    primary_license = reusability_type.upper()
                    if (
                        license_type != "Unknown"
                        and reusability_type == "open"
                    ):
                        primary_license = license_type

                    # Extract metadata with safe defaults
                    countries = item.get("country", ["Unknown"])
                    item_type = item.get("type", "Unknown")
                    year = extract_year_from_item(item)
                    providers = item.get("dataProvider", ["Unknown"])

                    # Update counters
                    license_counts[primary_license] += 1

                    # Count by country and license
                    for country in countries:
                        country_counts[primary_license][country] += 1

                    # Count by type and license
                    type_counts[primary_license][item_type] += 1

                    # Count by year and license
                    year_counts[primary_license][year] += 1

                    # Count by provider and license
                    for provider in providers:
                        provider_counts[primary_license][provider] += 1

                    total_fetched += 1
                    items_found_in_batch += 1

                    # Log progress
                    country_name = countries[0] if countries else "Unknown"
                    LOGGER.info(
                        f"Found {reusability_type}: {primary_license} - "
                        f"{country_name} - {item_type} - {year}"
                    )

                # Rate limiting between requests
                time.sleep(RATE_DELAY)

                # Check for consecutive empty results
                if items_found_in_batch == 0:
                    consecutive_empty_calls += 1
                    if consecutive_empty_calls >= 2:
                        LOGGER.info(
                            f"No new items found in 2 consecutive calls for "
                            f"query: {reusability_type}. Moving to next query."
                        )
                        break
                else:
                    consecutive_empty_calls = 0

    except requests.exceptions.RequestException as e:
        LOGGER.error(f"API request failed: {e}")
        LOGGER.info("Stopping execution due to API issues")

    except KeyboardInterrupt:
        LOGGER.info("Execution interrupted by user")

    # Save results if any data was collected
    if args.enable_save and total_fetched > 0:
        save_count_data(
            license_counts,
            country_counts,
            type_counts,
            year_counts,
            provider_counts,
        )

    LOGGER.info(f"Total items fetched: {total_fetched}")


def save_count_data(
    license_counts, country_counts, type_counts, year_counts, provider_counts
):
    """
    Save count data to CSV files with proper error handling.

    Args:
        license_counts (dict): License type counts
        country_counts (dict): Country-based counts
        type_counts (dict): Content type counts
        year_counts (dict): Year-based counts
        provider_counts (dict): Data provider counts
    """
    try:
        # Save license counts
        with open(
            FILE_EUROPEANA_COUNT, "w", newline="", encoding="utf-8"
        ) as file_obj:
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

        # Save country counts
        with open(
            FILE_EUROPEANA_COUNTRY, "w", newline="", encoding="utf-8"
        ) as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=HEADER_COUNTRY, dialect="unix"
            )
            writer.writeheader()
            for license_type, countries in country_counts.items():
                for country, count in countries.items():
                    writer.writerow(
                        {
                            "TOOL_IDENTIFIER": license_type,
                            "COUNTRY": country,
                            "COUNT": count,
                        }
                    )

        # Save type counts
        with open(
            FILE_EUROPEANA_TYPE, "w", newline="", encoding="utf-8"
        ) as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=HEADER_TYPE, dialect="unix"
            )
            writer.writeheader()
            for license_type, types in type_counts.items():
                for item_type, count in types.items():
                    writer.writerow(
                        {
                            "TOOL_IDENTIFIER": license_type,
                            "TYPE": item_type,
                            "COUNT": count,
                        }
                    )

        # Save year counts
        with open(
            FILE_EUROPEANA_YEAR, "w", newline="", encoding="utf-8"
        ) as file_obj:
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

        # Save provider counts (primary research output)
        with open(
            FILE_EUROPEANA_PROVIDER, "w", newline="", encoding="utf-8"
        ) as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=HEADER_PROVIDER, dialect="unix"
            )
            writer.writeheader()
            for license_type, providers in provider_counts.items():
                for provider, count in providers.items():
                    writer.writerow(
                        {
                            "DATA_PROVIDER": provider,
                            "LEGAL_TOOL": license_type,
                            "COUNT": count,
                        }
                    )

        LOGGER.info("Successfully saved all data files")

    except IOError as e:
        LOGGER.error(f"Failed to save data files: {e}")
        raise


def main():
    """Main function with comprehensive error handling."""
    try:
        args = parse_arguments()
        shared.paths_log(LOGGER, PATHS)
        shared.git_fetch_and_merge(args, PATHS["repo"])
        initialize_all_data_files(args)
        query_europeana(args)
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add and commit new Europeana CC license data for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])

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


if __name__ == "__main__":
    main()
