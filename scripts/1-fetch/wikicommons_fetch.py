#!/usr/bin/env python
"""
Fetch CC Legal Tool usage data from WikiCommons API.
"""

# Standard library
import argparse
import csv
import os
import re
import sys
import time
import textwrap
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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
__version__ = "1.0.0"
BASE_URL = "https://commons.wikimedia.org/w/api.php"
CC_LICENSE_CATEGORIES = [
    "CC BY 4.0",
    "CC BY-SA 4.0", 
    "CC BY-NC 4.0",
    "CC BY-NC-SA 4.0",
    "CC BY-NC-ND 4.0",
    "CC BY-ND 4.0",
    "CC BY 3.0",
    "CC BY-SA 3.0",
    "CC BY-NC 3.0",
    "CC BY-NC-SA 3.0",
    "CC BY-NC-ND 3.0",
    "CC BY-ND 3.0",
    "CC BY 2.5",
    "CC BY-SA 2.5",
    "CC BY-NC 2.5",
    "CC BY-NC-SA 2.5",
    "CC BY-NC-ND 2.5",
    "CC BY-ND 2.5",
    "CC BY 2.0",
    "CC BY-SA 2.0",
    "CC BY-NC 2.0",
    "CC BY-NC-SA 2.0",
    "CC BY-NC-ND 2.0",
    "CC BY-ND 2.0",
    "CC BY 1.0",
    "CC BY-SA 1.0",
    "CC BY-NC 1.0",
    "CC BY-NC-SA 1.0",
    "CC BY-NC-ND 1.0",
    "CC BY-ND 1.0",
    "CC0 1.0",
    "Public Domain Mark 1.0",
]


def license_to_wikicommons_category(license_name):
    """
    Convert human-readable license name to WikiCommons category format.

    WikiCommons uses hyphenated category names (e.g., "CC-BY-4.0") while
    we store human-readable names (e.g., "CC BY 4.0"). This function
    converts between the two formats.

    Args:
        license_name: Human-readable license name (e.g., "CC BY 4.0")

    Returns:
        str: WikiCommons category name (e.g., "CC-BY-4.0")
    """
    # Handle special case: "Public Domain Mark 1.0" -> "PDM-1.0"
    # Note: LICENSE_NORMALIZATION has "PDM-1.0": "PDM 1.0", but we use
    # "Public Domain Mark 1.0" in CC_LICENSE_CATEGORIES
    if license_name == "Public Domain Mark 1.0":
        return "PDM-1.0"

    # Create reverse mapping from LICENSE_NORMALIZATION
    # LICENSE_NORMALIZATION maps: "CC-BY-4.0" -> "CC BY 4.0"
    # So reverse_map maps: "CC BY 4.0" -> "CC-BY-4.0"
    reverse_map = {v: k for k, v in shared.LICENSE_NORMALIZATION.items()}

    # Use reverse mapping if available
    if license_name in reverse_map:
        return reverse_map[license_name]

    # If not found in mapping, return as-is (shouldn't happen)
    msg = (
        f"License '{license_name}' not found in normalization map, "
        "using as-is"
    )
    LOGGER.warning(msg)
    return license_name


FILE1_COUNT = shared.path_join(PATHS["data_phase"], "wikicommons_1_count.csv")
HEADER1_COUNT = ["LICENSE", "FILE_COUNT", "PAGE_COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])


def test_category_conversion():
    """
    Test the license_to_wikicommons_category() function to ensure
    conversions are working correctly.

    This function validates that all license names in CC_LICENSE_CATEGORIES
    convert to the expected WikiCommons category format.
    """
    LOGGER.info("Testing category conversion function...")

    # Expected mappings based on LICENSE_NORMALIZATION
    # Format: (input, expected_output, is_special_case)
    expected_mappings = [
        ("CC BY 4.0", "CC-BY-4.0", False),
        ("CC BY-SA 4.0", "CC-BY-SA-4.0", False),
        ("CC BY-NC 4.0", "CC-BY-NC-4.0", False),
        ("CC BY-NC-SA 4.0", "CC-BY-NC-SA-4.0", False),
        ("CC BY-NC-ND 4.0", "CC-BY-NC-ND-4.0", False),
        ("CC BY-ND 4.0", "CC-BY-ND-4.0", False),
        ("CC BY 3.0", "CC-BY-3.0", False),
        ("CC BY-SA 3.0", "CC-BY-SA-3.0", False),
        ("CC BY-NC 3.0", "CC-BY-NC-3.0", False),
        ("CC BY-NC-SA 3.0", "CC-BY-NC-SA-3.0", False),
        ("CC BY-NC-ND 3.0", "CC-BY-NC-ND-3.0", False),
        ("CC BY-ND 3.0", "CC-BY-ND-3.0", False),
        ("CC BY 2.5", "CC-BY-2.5", False),
        ("CC BY-SA 2.5", "CC-BY-SA-2.5", False),
        ("CC BY-NC 2.5", "CC-BY-NC-2.5", False),
        ("CC BY-NC-SA 2.5", "CC-BY-NC-SA-2.5", False),
        ("CC BY-NC-ND 2.5", "CC-BY-NC-ND-2.5", False),
        ("CC BY-ND 2.5", "CC-BY-ND-2.5", False),
        ("CC BY 2.0", "CC-BY-2.0", False),
        ("CC BY-SA 2.0", "CC-BY-SA-2.0", False),
        ("CC BY-NC 2.0", "CC-BY-NC-2.0", False),
        ("CC BY-NC-SA 2.0", "CC-BY-NC-SA-2.0", False),
        ("CC BY-NC-ND 2.0", "CC-BY-NC-ND-2.0", False),
        ("CC BY-ND 2.0", "CC-BY-ND-2.0", False),
        ("CC BY 1.0", "CC-BY-1.0", False),
        ("CC BY-SA 1.0", "CC-BY-SA-1.0", False),
        ("CC BY-NC 1.0", "CC-BY-NC-1.0", False),
        ("CC BY-NC-SA 1.0", "CC-BY-NC-SA-1.0", False),
        ("CC BY-NC-ND 1.0", "CC-BY-NC-ND-1.0", False),
        ("CC BY-ND 1.0", "CC-BY-ND-1.0", False),
        ("CC0 1.0", "CC0-1.0", True),  # Special case: CC0
        ("Public Domain Mark 1.0", "PDM-1.0", True),  # Special case: PDM
    ]

    # Test all licenses in CC_LICENSE_CATEGORIES
    all_passed = True
    special_cases_found = []
    failures = []

    for input_license, expected_output, is_special in expected_mappings:
        actual_output = license_to_wikicommons_category(input_license)

        if actual_output == expected_output:
            status = "✓"
            if is_special:
                special_cases_found.append(
                    (input_license, expected_output)
                )
        else:
            status = "✗"
            all_passed = False
            failures.append(
                (input_license, expected_output, actual_output)
            )

        LOGGER.debug(
            f"{status} {input_license:25} -> {actual_output:20} "
            f"(expected: {expected_output})"
        )

    # Log summary
    if special_cases_found:
        LOGGER.info(
            f"Found {len(special_cases_found)} special cases that need "
            "explicit handling:"
        )
        for input_license, output in special_cases_found:
            LOGGER.info(f"  '{input_license}' -> '{output}'")

    if failures:
        LOGGER.error(f"Conversion test failed for {len(failures)} licenses:")
        for input_license, expected, actual in failures:
            LOGGER.error(
                f"  '{input_license}' -> expected '{expected}', "
                f"got '{actual}'"
            )
        raise shared.QuantifyingException(
            "Category conversion test failed. Please fix the conversion "
            "function before proceeding.",
            1
        )

    if all_passed:
        LOGGER.info(
            f"✓ All {len(expected_mappings)} category conversions passed!"
        )

    # Test some edge cases
    LOGGER.info("Testing edge cases...")
    edge_cases = [
        ("Unknown License", "Unknown License"),  # Should return as-is
    ]

    for input_license, expected_behavior in edge_cases:
        actual_output = license_to_wikicommons_category(input_license)
        if actual_output == expected_behavior:
            LOGGER.debug(
                f"✓ Edge case handled: '{input_license}' -> "
                f"'{actual_output}' (as expected)"
            )
        else:
            LOGGER.warning(
                f"Edge case '{input_license}' returned '{actual_output}' "
                f"(expected '{expected_behavior}')"
            )

    return all_passed


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
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
        "--debug",
        action="store_true",
        help="Enable detailed debug logging including API URLs and raw responses",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process only first N licenses (for quick testing)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip category validation (for faster re-runs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test conversions and validation without running full queries",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        metavar="SECONDS",
        help="Set custom timeout for API calls (default: 60)",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        metavar="N",
        help="Set maximum recursion depth (default: 4)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        metavar="N",
        help="Enable parallel processing with N workers (default: 4)",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.timeout < 1:
        parser.error("--timeout must be at least 1 second")
    if args.max_depth < 1:
        parser.error("--max-depth must be at least 1")
    if args.parallel < 1:
        parser.error("--parallel must be at least 1")
    return args


def check_for_completion():
    """Check if data fetch is already completed for this quarter."""
    try:
        with open(FILE1_COUNT, "r", encoding="utf-8") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            if len(list(reader)) >= len(CC_LICENSE_CATEGORIES):
                raise shared.QuantifyingException(
                    f"Data fetch completed for {QUARTER}", 0
                )
    except FileNotFoundError:
        pass  # File may not be found without --enable-save, etc.


# Constants for timeout and retry configuration (defaults, can be overridden)
CATEGORY_COUNT_TIMEOUT = 300  # 5 minutes timeout for entire category count
INITIAL_BACKOFF = 2  # Initial backoff in seconds (exponential)
MAX_RETRIES = 3  # Maximum retries for failed requests

# Global cache for category counts (shared across all license queries)
# Format: {category_name: {"files": int, "pages": int, "timestamp": float}}
_category_cache = {}
_cache_lock = threading.Lock()

# Performance tracking
_cache_hits = 0
_cache_misses = 0
_perf_lock = threading.Lock()


def get_requests_session(request_timeout=60, debug=False):
    """
    Create a requests session with retry logic and timeout handling.
    
    Args:
        request_timeout: Timeout for API requests in seconds
        debug: Enable debug logging for API calls
    
    Returns:
        requests.Session: Configured session with retry and timeout
    """
    max_retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=INITIAL_BACKOFF,
        # Add rate limit (429) to retry status list
        status_forcelist=shared.RETRY_STATUS_FORCELIST + [429],
        allowed_methods=["GET"],  # Only retry GET requests
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    headers = {
        "User-Agent": shared.USER_AGENT
    }
    session.headers.update(headers)
    
    # Store timeout and debug flag in session for later use
    session._request_timeout = request_timeout
    session._debug = debug
    
    return session


def check_category_exists(session, category_name):
    """
    Check if a category exists on WikiCommons.
    
    This is a lightweight check that only verifies existence,
    not the full category info.
    
    Args:
        session: requests.Session object
        category_name: Name of the category to check
        
    Returns:
        bool: True if category exists, False otherwise
    """
    params = {
        "action": "query",
        "titles": f"Category:{category_name}",
        "format": "json"
    }
    
    timeout = getattr(session, '_request_timeout', 60)
    debug = getattr(session, '_debug', False)
    
    url = BASE_URL
    if debug:
        LOGGER.debug(f"Checking category existence: {url}")
        LOGGER.debug(f"  Params: {params}")
    
    try:
        with session.get(url, params=params, timeout=timeout) as response:
            if debug:
                LOGGER.debug(
                    f"  Response status: {response.status_code}"
                )
                LOGGER.debug(f"  Response headers: {dict(response.headers)}")
            response.raise_for_status()
            data = response.json()
            if debug:
                LOGGER.debug(f"  Response data: {data}")
            
        pages = data.get("query", {}).get("pages", {})
        if not pages:
            return False
            
        # Check if page ID is -1 (missing page)
        page_id = list(pages.keys())[0]
        return page_id != "-1"
        
    except (requests.HTTPError, requests.RequestException, KeyError) as e:
        if debug:
            LOGGER.debug(f"  Error: {e}")
        return False


def suggest_alternative_category_name(category_name):
    """
    Suggest alternative category names if the original doesn't exist.
    
    Args:
        category_name: The category name that wasn't found
        
    Returns:
        list: List of suggested alternative names
    """
    suggestions = []
    
    # Common variations to try
    variations = [
        category_name.replace("-", " "),  # Hyphens to spaces
        category_name.replace(" ", "-"),  # Spaces to hyphens
        category_name.upper(),  # All caps
        category_name.lower(),  # All lowercase
    ]
    
    # Remove duplicates and the original
    for variation in variations:
        if variation != category_name and variation not in suggestions:
            suggestions.append(variation)
    
    return suggestions


def validate_categories_exist(session):
    """
    Validate that all converted WikiCommons categories actually exist.
    
    This lightweight check prevents expensive recursive queries on
    non-existent categories.
    
    Args:
        session: requests.Session object
        
    Returns:
        list: List of tuples (license_name, category_name) for valid
              categories that exist on WikiCommons
    """
    LOGGER.info("Validating category existence on WikiCommons...")
    
    valid_categories = []
    missing_categories = []
    
    for license_name in CC_LICENSE_CATEGORIES:
        # Convert to WikiCommons format
        category_name = license_to_wikicommons_category(license_name)
        
        # Check if category exists
        exists = check_category_exists(session, category_name)
        
        if exists:
            valid_categories.append((license_name, category_name))
            LOGGER.debug(
                f"✓ Category exists: '{category_name}' "
                f"(for license: '{license_name}')"
            )
        else:
            missing_categories.append((license_name, category_name))
            LOGGER.warning(
                f"✗ Category not found: '{category_name}' "
                f"(for license: '{license_name}')"
            )
            
            # Suggest alternatives
            suggestions = suggest_alternative_category_name(category_name)
            if suggestions:
                LOGGER.info(
                    f"  Suggestions for '{category_name}': "
                    f"{', '.join(suggestions[:3])}"
                )
    
    # Summary
    total = len(CC_LICENSE_CATEGORIES)
    valid_count = len(valid_categories)
    missing_count = len(missing_categories)
    
    LOGGER.info(
        f"Category validation complete: {valid_count}/{total} categories "
        f"exist, {missing_count} missing"
    )
    
    if missing_categories:
        LOGGER.warning(
            f"Found {missing_count} missing categories. These will be "
            "skipped or may return zero counts:"
        )
        for license_name, category_name in missing_categories:
            LOGGER.warning(f"  - {license_name} -> {category_name}")
    
    if valid_count == 0:
        raise shared.QuantifyingException(
            "No valid categories found! Please check category names and "
            "conversion function.",
            1
        )
    
    return valid_categories


def get_category_info(session, category_name, retry_count=0):
    """
    Get file and page count for a specific category with retry logic.
    
    Args:
        session: requests.Session object
        category_name: Name of the category to query
        retry_count: Current retry attempt (for exponential backoff)
        
    Returns:
        dict: Dictionary with 'files' and 'pages' counts
    """
    params = {
        "action": "query",
        "prop": "categoryinfo",
        "titles": f"Category:{category_name}",
        "format": "json"
    }
    
    timeout = getattr(session, '_request_timeout', 60)
    debug = getattr(session, '_debug', False)
    
    url = BASE_URL
    if debug:
        LOGGER.debug(f"Getting category info: {url}")
        LOGGER.debug(f"  Category: {category_name}")
        LOGGER.debug(f"  Params: {params}")
        LOGGER.debug(f"  Retry count: {retry_count}")
    
    try:
        with session.get(url, params=params, timeout=timeout) as response:
            if debug:
                LOGGER.debug(
                    f"  Response status: {response.status_code}"
                )
                LOGGER.debug(f"  Response headers: {dict(response.headers)}")
            # Handle rate limiting
            if response.status_code == 429:
                default_backoff = INITIAL_BACKOFF ** (retry_count + 1)
                retry_after = int(
                    response.headers.get("Retry-After", default_backoff)
                )
                if retry_count < MAX_RETRIES:
                    LOGGER.warning(
                        f"Rate limited. Waiting {retry_after}s before retry "
                        f"({retry_count + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(retry_after)
                    return get_category_info(session, category_name, retry_count + 1)
                else:
                    raise shared.QuantifyingException(
                        f"Rate limit exceeded after {MAX_RETRIES} retries", 1
                    )
            
            response.raise_for_status()
            data = response.json()
            
        pages = data.get("query", {}).get("pages", {})
        if not pages:
            LOGGER.warning(f"No data found for category: {category_name}")
            return {"files": 0, "pages": 0}
            
        # Get the first (and usually only) page result
        page_data = list(pages.values())[0]
        categoryinfo = page_data.get("categoryinfo", {})
        
        files = categoryinfo.get("files", 0)
        pages = categoryinfo.get("pages", 0)
        
        LOGGER.debug(
            f"Category {category_name}: {files} files, {pages} pages"
        )
        return {"files": files, "pages": pages}
        
    except requests.Timeout:
        if retry_count < MAX_RETRIES:
            backoff = INITIAL_BACKOFF ** (retry_count + 1)
            LOGGER.warning(
                f"Request timeout for {category_name}. "
                f"Retrying in {backoff}s ({retry_count + 1}/{MAX_RETRIES})..."
            )
            time.sleep(backoff)
            return get_category_info(session, category_name, retry_count + 1)
        else:
            LOGGER.error(
                f"Request timeout after {MAX_RETRIES} retries for "
                f"{category_name}"
            )
            return {"files": 0, "pages": 0}
    except requests.HTTPError as e:
        retryable_statuses = [429, 500, 502, 503, 504]
        if retry_count < MAX_RETRIES and e.response.status_code in retryable_statuses:
            backoff = INITIAL_BACKOFF ** (retry_count + 1)
            LOGGER.warning(
                f"HTTP {e.response.status_code} for {category_name}. "
                f"Retrying in {backoff}s ({retry_count + 1}/{MAX_RETRIES})..."
            )
            time.sleep(backoff)
            return get_category_info(session, category_name, retry_count + 1)
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        if retry_count < MAX_RETRIES:
            backoff = INITIAL_BACKOFF ** (retry_count + 1)
            LOGGER.warning(
                f"Request exception for {category_name}: {e}. "
                f"Retrying in {backoff}s ({retry_count + 1}/{MAX_RETRIES})..."
            )
            time.sleep(backoff)
            return get_category_info(session, category_name, retry_count + 1)
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        raise shared.QuantifyingException(f"KeyError: {e}", 1)


def get_subcategories(session, category_name, retry_count=0):
    """
    Get subcategories for a given category with retry logic.
    
    Args:
        session: requests.Session object
        category_name: Name of the parent category
        retry_count: Current retry attempt (for exponential backoff)
        
    Returns:
        list: List of subcategory names
    """
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Category:{category_name}",
        "cmtype": "subcat",
        "cmlimit": "500",  # Maximum allowed
        "format": "json"
    }
    
    timeout = getattr(session, '_request_timeout', 60)
    debug = getattr(session, '_debug', False)
    
    url = BASE_URL
    if debug:
        LOGGER.debug(f"Getting subcategories: {url}")
        LOGGER.debug(f"  Category: {category_name}")
        LOGGER.debug(f"  Params: {params}")
        LOGGER.debug(f"  Retry count: {retry_count}")
    
    try:
        with session.get(url, params=params, timeout=timeout) as response:
            if debug:
                LOGGER.debug(
                    f"  Response status: {response.status_code}"
                )
                LOGGER.debug(f"  Response headers: {dict(response.headers)}")
            # Handle rate limiting
            if response.status_code == 429:
                default_backoff = INITIAL_BACKOFF ** (retry_count + 1)
                retry_after = int(
                    response.headers.get("Retry-After", default_backoff)
                )
                if retry_count < MAX_RETRIES:
                    LOGGER.warning(
                        f"Rate limited getting subcategories. "
                        f"Waiting {retry_after}s "
                        f"({retry_count + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(retry_after)
                    return get_subcategories(session, category_name, retry_count + 1)
                else:
                    LOGGER.error(
                        f"Rate limit exceeded after {MAX_RETRIES} retries"
                    )
                    return []
            
            response.raise_for_status()
            data = response.json()
            if debug:
                LOGGER.debug(f"  Response data: {data}")
            
        members = data.get("query", {}).get("categorymembers", [])
        subcategories = []
        
        for member in members:
            title = member.get("title", "")
            if title.startswith("Category:"):
                subcat_name = title.replace("Category:", "")
                subcategories.append(subcat_name)
                
        LOGGER.debug(
            f"Found {len(subcategories)} subcategories for {category_name}"
        )
        return subcategories
        
    except requests.Timeout:
        if retry_count < MAX_RETRIES:
            backoff = INITIAL_BACKOFF ** (retry_count + 1)
            LOGGER.warning(
                f"Timeout getting subcategories for {category_name}. "
                f"Retrying in {backoff}s ({retry_count + 1}/{MAX_RETRIES})..."
            )
            time.sleep(backoff)
            return get_subcategories(session, category_name, retry_count + 1)
        else:
            LOGGER.error(
                f"Timeout after {MAX_RETRIES} retries for {category_name}"
            )
            return []
    except requests.HTTPError as e:
        retryable_statuses = [429, 500, 502, 503, 504]
        if retry_count < MAX_RETRIES and e.response.status_code in retryable_statuses:
            backoff = INITIAL_BACKOFF ** (retry_count + 1)
            LOGGER.warning(
                f"HTTP {e.response.status_code} getting subcategories. "
                f"Retrying in {backoff}s "
                f"({retry_count + 1}/{MAX_RETRIES})..."
            )
            time.sleep(backoff)
            return get_subcategories(session, category_name, retry_count + 1)
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        if retry_count < MAX_RETRIES:
            backoff = INITIAL_BACKOFF ** (retry_count + 1)
            LOGGER.warning(
                f"Request exception getting subcategories: {e}. "
                f"Retrying in {backoff}s ({retry_count + 1}/{MAX_RETRIES})..."
            )
            time.sleep(backoff)
            return get_subcategories(session, category_name, retry_count + 1)
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        raise shared.QuantifyingException(f"KeyError: {e}", 1)


def get_cached_category_count(category_name):
    """
    Get cached category count if available.
    
    Args:
        category_name: Name of the category
        
    Returns:
        dict or None: Cached count dict or None if not cached
    """
    with _cache_lock:
        result = _category_cache.get(category_name)
        if result is not None:
            with _perf_lock:
                global _cache_hits
                _cache_hits += 1
        else:
            with _perf_lock:
                global _cache_misses
                _cache_misses += 1
        return result


def set_cached_category_count(category_name, counts):
    """
    Cache category count for future use.
    
    Args:
        category_name: Name of the category
        counts: Dictionary with 'files' and 'pages' counts
    """
    with _cache_lock:
        _category_cache[category_name] = {
            "files": counts["files"],
            "pages": counts["pages"],
            "timestamp": time.time()
        }


def clear_category_cache():
    """Clear the global category cache."""
    with _cache_lock:
        _category_cache.clear()
    LOGGER.info("Category cache cleared")


def get_cache_stats():
    """
    Get statistics about the category cache.
    
    Returns:
        dict: Cache statistics
    """
    with _cache_lock:
        with _perf_lock:
            total_requests = _cache_hits + _cache_misses
            hit_rate = (
                (_cache_hits / total_requests * 100)
                if total_requests > 0 else 0
            )
            return {
                "size": len(_category_cache),
                "categories": list(_category_cache.keys()),
                "hits": _cache_hits,
                "misses": _cache_misses,
                "hit_rate": hit_rate
            }


def recursively_count_category(
    session,
    category_name,
    visited=None,
    depth=0,
    max_retries=MAX_RETRIES,
    start_time=None,
    max_depth=4
):
    """
    Recursively count files and pages in a category and its subcategories.
    
    Uses global caching to avoid duplicate work across different license
    queries. Implements recursion depth limiting to prevent infinite recursion.
    
    Args:
        session: requests.Session object
        category_name: Name of the category to count (WikiCommons format)
        visited: Set of already visited categories (for cycle detection)
        depth: Current recursion depth
        max_retries: Maximum number of retries for failed operations
        start_time: Start time for timeout checking (None = no timeout)
        max_depth: Maximum recursion depth (default: 4)
        
    Returns:
        dict: Dictionary with 'files' and 'pages' counts
    """
    # Check timeout for entire category count operation
    if start_time is not None:
        elapsed = time.time() - start_time
        if elapsed > CATEGORY_COUNT_TIMEOUT:
            LOGGER.warning(
                f"Category count timeout ({CATEGORY_COUNT_TIMEOUT}s) "
                f"for {category_name} at depth {depth}"
            )
            return {"files": 0, "pages": 0}
    
    # Check recursion depth limit
    if depth >= max_depth:
        LOGGER.debug(
            f"Max recursion depth ({max_depth}) reached for "
            f"{category_name}. Stopping recursion."
        )
        # Still get direct counts for this level
        try:
            counts = get_category_info(session, category_name)
            return counts if counts else {"files": 0, "pages": 0}
        except Exception:
            return {"files": 0, "pages": 0}
    
    # Check global cache first
    cached = get_cached_category_count(category_name)
    if cached is not None:
        LOGGER.debug(
            f"Using cached result for {category_name}: "
            f"{cached['files']} files, {cached['pages']} pages"
        )
        return {"files": cached["files"], "pages": cached["pages"]}
    
    if visited is None:
        visited = set()
        
    if category_name in visited:
        LOGGER.warning(f"Cycle detected for category: {category_name}")
        return {"files": 0, "pages": 0}
        
    visited.add(category_name)
    
    # Get direct counts for this category with retry logic
    try:
        counts = get_category_info(session, category_name)
    except Exception as e:
        LOGGER.error(
            f"Failed to get category info for {category_name}: {e}"
        )
        return {"files": 0, "pages": 0}
    
    # Handle case where category doesn't exist (returns None)
    if counts is None:
        LOGGER.warning(f"Category {category_name} not found or has no data")
        return {"files": 0, "pages": 0}
    
    # Get subcategories and recursively count them
    try:
        subcategories = get_subcategories(session, category_name)
    except Exception as e:
        LOGGER.error(
            f"Failed to get subcategories for {category_name}: {e}"
        )
        # Cache and return what we have so far
        set_cached_category_count(category_name, counts)
        return counts
    
    # Recursively count subcategories (with depth limit)
    # Note: Don't use visited.copy() - share the same visited set to prevent
    # revisiting categories across different branches
    for subcat in subcategories:
        if subcat not in visited:  # Avoid infinite recursion
            try:
                subcat_counts = recursively_count_category(
                    session,
                    subcat,
                    visited,
                    depth + 1,
                    max_retries,
                    start_time,
                    max_depth
                )
                counts["files"] += subcat_counts["files"]
                counts["pages"] += subcat_counts["pages"]
            except Exception as e:
                LOGGER.warning(
                    f"Failed to count subcategory {subcat}: {e}. "
                    "Continuing with other subcategories..."
                )
                # Continue with other subcategories
    
    # Cache the result for future use
    set_cached_category_count(category_name, counts)
    
    return counts


def write_data(args, license_data):
    """Write the collected data to CSV file."""
    if not args.enable_save:
        return args

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if len(license_data) < len(CC_LICENSE_CATEGORIES):
        LOGGER.error("Unable to fetch all records. Aborting.")
        return args

    with open(FILE1_COUNT, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER1_COUNT, dialect="unix"
        )
        writer.writeheader()
        for row in license_data:
            writer.writerow(row)
    
    LOGGER.info(f"Data written to {FILE1_COUNT}")
    return args


def format_time_remaining(seconds):
    """
    Format time remaining in a human-readable format.
    
    Args:
        seconds: Number of seconds remaining
        
    Returns:
        str: Formatted time string (e.g., "2h 30m 15s")
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours}h {minutes}m {secs}s"


def print_startup_banner(args):
    """
    Print a professional startup banner with script information.
    
    Args:
        args: Parsed command-line arguments
    """
    LOGGER.info("=" * 80)
    LOGGER.info("WikiCommons CC License Data Fetcher")
    LOGGER.info(f"Version {__version__}")
    LOGGER.info("=" * 80)
    LOGGER.info("Configuration:")
    LOGGER.info(f"  Quarter: {QUARTER}")
    LOGGER.info(f"  Timeout: {args.timeout}s")
    LOGGER.info(f"  Max Depth: {args.max_depth}")
    LOGGER.info(f"  Parallel Workers: {args.parallel}")
    if args.limit:
        LOGGER.info(f"  Limit: {args.limit} licenses (testing mode)")
    if args.skip_validation:
        LOGGER.info("  Validation: SKIPPED")
    if args.debug:
        LOGGER.info("  Debug Mode: ENABLED")
    if args.dry_run:
        LOGGER.info("  Mode: DRY RUN")
    LOGGER.info("=" * 80)


def process_single_license(
    session, license_name, category_name, index, total_licenses, start_time,
    max_depth=4
):
    """
    Process a single license category count.
    
    This function is designed to be called in parallel by ThreadPoolExecutor.
    
    Args:
        session: requests.Session object (thread-safe)
        license_name: Human-readable license name
        category_name: WikiCommons category name
        index: Current index (for progress reporting)
        total_licenses: Total number of licenses
        start_time: Start time of overall operation
        
    Returns:
        tuple: (license_name, result_dict, elapsed_time, success)
    """
    license_start_time = time.time()
    progress_pct = (index / total_licenses) * 100
    
    LOGGER.info(
        f"[{index}/{total_licenses}] ({progress_pct:.1f}%) "
        f"Processing: {license_name}"
    )
    LOGGER.info(f"  Category: {category_name}")
    
    try:
        # Use timeout for entire category count
        counts = recursively_count_category(
            session,
            category_name,
            visited=None,
            depth=0,
            start_time=license_start_time,
            max_depth=max_depth
        )
        license_elapsed = time.time() - license_start_time
        
        LOGGER.info(
            f"  ✓ Completed in {license_elapsed:.1f}s - "
            f"{counts['files']:,} files, {counts['pages']:,} pages"
        )
        
        return (
            license_name,
            {
                "LICENSE": license_name,
                "FILE_COUNT": counts["files"],
                "PAGE_COUNT": counts["pages"]
            },
            license_elapsed,
            True,
            None
        )
        
    except Exception as e:
        license_elapsed = time.time() - license_start_time
        LOGGER.error(
            f"  ✗ Failed after {license_elapsed:.1f}s: {e}"
        )
        
        return (
            license_name,
            {
                "LICENSE": license_name,
                "FILE_COUNT": 0,
                "PAGE_COUNT": 0
            },
            license_elapsed,
            False,
            str(e)
        )


def query_wikicommons(args, session, valid_categories):
    """
    Query WikiCommons API for CC license data with progress tracking.
    
    Uses parallel processing with ThreadPoolExecutor for better performance.
    Uses global caching to avoid duplicate category queries.
    
    Args:
        args: Parsed command-line arguments
        session: requests.Session object
        valid_categories: List of tuples (license_name, category_name) for
                         categories that have been validated to exist
        
    Returns:
        tuple: (license_data list, summary dict with success/failure counts)
    """
    # Apply limit if specified
    if args.limit is not None:
        valid_categories = valid_categories[:args.limit]
        LOGGER.info(f"Limited to first {args.limit} licenses for testing")
    
    total_licenses = len(valid_categories)
    start_time = time.time()
    success_count = 0
    failure_count = 0
    failed_licenses = []
    license_data = []
    
    # Clear cache at start (optional - can be removed to persist cache)
    # clear_category_cache()
    
    max_workers = args.parallel
    max_depth = args.max_depth
    
    LOGGER.info(
        f"Starting to process {total_licenses} licenses "
        f"with {max_workers} parallel workers..."
    )
    LOGGER.info(f"Using max recursion depth: {max_depth}")
    LOGGER.info(
        f"Using cache: {len(_category_cache)} categories already cached"
    )
    
    # Use ThreadPoolExecutor for parallel processing
    # Note: requests.Session is not fully thread-safe, so we create
    # a session per thread
    def create_session():
        """Create a new session for each thread."""
        return get_requests_session(
            request_timeout=args.timeout,
            debug=args.debug
        )
    
    # Process licenses in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_license = {}
        for index, (license_name, category_name) in enumerate(
            valid_categories, 1
        ):
            # Create a new session for each task to ensure thread safety
            task_session = create_session()
            future = executor.submit(
                process_single_license,
                task_session,
                license_name,
                category_name,
                index,
                total_licenses,
                start_time,
                max_depth
            )
            future_to_license[future] = (license_name, index)
        
        # Process completed tasks as they finish
        completed = 0
        for future in as_completed(future_to_license):
            completed += 1
            license_name, index = future_to_license[future]
            
            try:
                (
                    result_license_name,
                    result_data,
                    elapsed,
                    success,
                    error
                ) = future.result()
                
                license_data.append(result_data)
                
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                    if error:
                        failed_licenses.append((license_name, error))
                
                # Log progress
                remaining = total_licenses - completed
                if remaining > 0:
                    avg_time = (time.time() - start_time) / completed
                    estimated_remaining = avg_time * remaining
                    time_remaining_str = format_time_remaining(
                        estimated_remaining
                    )
                    LOGGER.info(
                        f"Progress: {completed}/{total_licenses} completed. "
                        f"Estimated time remaining: {time_remaining_str}"
                    )
                    
            except Exception as e:
                failure_count += 1
                failed_licenses.append((license_name, str(e)))
                license_data.append({
                    "LICENSE": license_name,
                    "FILE_COUNT": 0,
                    "PAGE_COUNT": 0
                })
                LOGGER.error(
                    f"Unexpected error processing {license_name}: {e}"
                )
    
    # Sort license_data by original order
    license_dict = {item["LICENSE"]: item for item in license_data}
    license_data = [
        license_dict.get(license_name, {
            "LICENSE": license_name,
            "FILE_COUNT": 0,
            "PAGE_COUNT": 0
        })
        for license_name, _ in valid_categories
    ]
    
    # Add entries for missing categories with zero counts
    processed_licenses = {
        license_name for license_name, _ in valid_categories
    }
    skipped_licenses = []
    for license_name in CC_LICENSE_CATEGORIES:
        if license_name not in processed_licenses:
            skipped_licenses.append(license_name)
            LOGGER.warning(
                f"Skipping '{license_name}' - category not found on "
                "WikiCommons"
            )
            license_data.append({
                "LICENSE": license_name,
                "FILE_COUNT": 0,
                "PAGE_COUNT": 0
            })
    
    total_time = time.time() - start_time
    
    # Calculate total files and pages
    total_files = sum(item.get("FILE_COUNT", 0) for item in license_data)
    total_pages = sum(item.get("PAGE_COUNT", 0) for item in license_data)
    
    # Get cache statistics
    cache_stats = get_cache_stats()
    
    summary = {
        "total": total_licenses,
        "success": success_count,
        "failure": failure_count,
        "failed_licenses": failed_licenses,
        "skipped": len(skipped_licenses),
        "skipped_licenses": skipped_licenses,
        "total_time": total_time,
        "avg_time_per_license": (
            total_time / total_licenses if total_licenses > 0 else 0
        ),
        "total_files": total_files,
        "total_pages": total_pages,
        "cache_size": cache_stats["size"],
        "cache_hits": cache_stats["hits"],
        "cache_misses": cache_stats["misses"],
        "cache_hit_rate": cache_stats["hit_rate"]
    }
    
    return license_data, summary


def check_popular_licenses(license_data):
    """
    Check that popular licenses have reasonable file counts.
    
    Args:
        license_data: List of dicts with LICENSE and FILE_COUNT keys
        
    Returns:
        list: List of warnings for licenses with suspiciously low counts
    """
    warnings = []
    popular_licenses = {
        "CC BY 4.0": 1000,
        "CC BY-SA 4.0": 1000,
        "CC0 1.0": 1000,
    }
    
    license_dict = {item["LICENSE"]: item for item in license_data}
    
    for license_name, min_count in popular_licenses.items():
        if license_name in license_dict:
            file_count = license_dict[license_name]["FILE_COUNT"]
            if file_count < min_count:
                warnings.append(
                    f"⚠️  {license_name} has only {file_count:,} files "
                    f"(expected at least {min_count:,})"
                )
    
    return warnings


def extract_license_version(license_name):
    """
    Extract version number from license name.
    
    Args:
        license_name: License name like "CC BY 4.0" or "CC0 1.0"
        
    Returns:
        float or None: Version number (e.g., 4.0, 3.0, 1.0) or None
    """
    # Match version patterns like "4.0", "3.0", "1.0", "2.5", "2.0"
    match = re.search(r'(\d+\.\d+)', license_name)
    if match:
        return float(match.group(1))
    return None


def extract_license_type(license_name):
    """
    Extract license type from license name.
    
    Args:
        license_name: License name like "CC BY 4.0" or "CC BY-SA 3.0"
        
    Returns:
        str: License type (e.g., "CC BY", "CC BY-SA", "CC0", "PDM")
    """
    if license_name.startswith("CC0"):
        return "CC0"
    if license_name.startswith("Public Domain Mark"):
        return "PDM"
    # Extract base type (e.g., "CC BY", "CC BY-SA", "CC BY-NC")
    parts = license_name.split()
    if len(parts) >= 2:
        if parts[1] == "BY":
            if len(parts) >= 3 and parts[2] in ["SA", "NC", "ND"]:
                return f"CC BY-{parts[2]}"
            return "CC BY"
        elif parts[1] == "BY-SA":
            return "CC BY-SA"
        elif parts[1] == "BY-NC":
            if len(parts) >= 3 and parts[2] == "SA":
                return "CC BY-NC-SA"
            elif len(parts) >= 3 and parts[2] == "ND":
                return "CC BY-NC-ND"
            return "CC BY-NC"
        elif parts[1] == "BY-ND":
            return "CC BY-ND"
    return "Unknown"


def verify_version_progression(license_data):
    """
    Verify that newer license versions generally have more files.
    
    Args:
        license_data: List of dicts with LICENSE and FILE_COUNT keys
        
    Returns:
        list: List of warnings for version progression anomalies
    """
    warnings = []
    
    # Group licenses by type
    license_dict = {item["LICENSE"]: item for item in license_data}
    license_groups = {}
    
    for license_name, data in license_dict.items():
        license_type = extract_license_type(license_name)
        version = extract_license_version(license_name)
        
        if license_type and version:
            if license_type not in license_groups:
                license_groups[license_type] = []
            license_groups[license_type].append({
                "name": license_name,
                "version": version,
                "count": data["FILE_COUNT"]
            })
    
    # Check each license type group
    for license_type, licenses in license_groups.items():
        # Sort by version (newest first)
        licenses.sort(key=lambda x: x["version"], reverse=True)
        
        # Check that newer versions generally have more files
        for i in range(len(licenses) - 1):
            newer = licenses[i]
            older = licenses[i + 1]
            
            # Allow some tolerance (older can be up to 50% of newer)
            # but flag if older has significantly more
            if older["count"] > newer["count"] * 1.5:
                warnings.append(
                    f"⚠️  Version anomaly: {older['name']} "
                    f"({older['count']:,} files) has more than "
                    f"{newer['name']} ({newer['count']:,} files)"
                )
    
    return warnings


def compare_license_counts(license_data):
    """
    Compare counts across licenses and flag anomalies.
    
    Args:
        license_data: List of dicts with LICENSE and FILE_COUNT keys
        
    Returns:
        list: List of warnings for count anomalies
    """
    warnings = []
    license_dict = {item["LICENSE"]: item for item in license_data}
    
    # Calculate statistics
    counts = [item["FILE_COUNT"] for item in license_data]
    if not counts:
        return warnings
    
    max_count = max(counts)
    min_count = min(counts)
    avg_count = sum(counts) / len(counts)
    
    # Flag licenses with unusually high or low counts
    for license_name, data in license_dict.items():
        count = data["FILE_COUNT"]
        
        # Flag if count is unusually high (more than 10x average)
        if count > avg_count * 10 and avg_count > 0:
            warnings.append(
                f"⚠️  {license_name} has unusually high count: "
                f"{count:,} files (avg: {avg_count:,.0f})"
            )
        
        # Flag if count is zero for non-special licenses
        if count == 0 and license_name not in ["CC0 1.0", "Public Domain Mark 1.0"]:
            # Check if it's an old version (1.0, 2.0) which might legitimately be 0
            version = extract_license_version(license_name)
            if version and version >= 2.5:
                warnings.append(
                    f"⚠️  {license_name} has zero files (may indicate "
                    "category issue)"
                )
    
    return warnings


def validate_special_licenses(license_data):
    """
    Validate that CC0-1.0 and PDM-1.0 return reasonable file counts.
    
    Args:
        license_data: List of dicts with LICENSE and FILE_COUNT keys
        
    Returns:
        list: List of warnings for special license issues
    """
    warnings = []
    license_dict = {item["LICENSE"]: item for item in license_data}
    
    special_licenses = {
        "CC0 1.0": {
            "min_reasonable": 10000,  # CC0 should have many files
            "description": "CC0"
        },
        "Public Domain Mark 1.0": {
            "min_reasonable": 1000,  # PDM should have some files
            "description": "Public Domain Mark"
        }
    }
    
    for license_name, criteria in special_licenses.items():
        if license_name in license_dict:
            count = license_dict[license_name]["FILE_COUNT"]
            min_reasonable = criteria["min_reasonable"]
            
            if count < min_reasonable:
                warnings.append(
                    f"⚠️  {criteria['description']} ({license_name}) has "
                    f"only {count:,} files (expected at least "
                    f"{min_reasonable:,})"
                )
            elif count == 0:
                warnings.append(
                    f"⚠️  {criteria['description']} ({license_name}) has "
                    "zero files - this may indicate a category naming issue"
                )
    
    return warnings


def validate_total_counts(license_data):
    """
    Validate that total file counts make sense (should be millions).
    
    Args:
        license_data: List of dicts with LICENSE and FILE_COUNT keys
        
    Returns:
        list: List of warnings for total count issues
    """
    warnings = []
    
    total_files = sum(item["FILE_COUNT"] for item in license_data)
    
    # WikiCommons should have millions of CC-licensed files
    if total_files < 1_000_000:
        warnings.append(
            f"⚠️  Total file count ({total_files:,}) is suspiciously low. "
            "Expected millions of files on WikiCommons."
        )
    elif total_files < 5_000_000:
        warnings.append(
            f"⚠️  Total file count ({total_files:,}) is lower than "
            "expected. WikiCommons typically has tens of millions of "
            "CC-licensed files."
        )
    else:
        LOGGER.info(
            f"✓ Total file count: {total_files:,} files (reasonable)"
        )
    
    return warnings


def perform_sanity_checks(license_data):
    """
    Perform all sanity checks and validations on license data.
    
    Args:
        license_data: List of dicts with LICENSE and FILE_COUNT keys
        
    Returns:
        dict: Dictionary with all warnings organized by category
    """
    LOGGER.info("Performing sanity checks and validations...")
    
    checks = {
        "popular_licenses": check_popular_licenses(license_data),
        "version_progression": verify_version_progression(license_data),
        "count_anomalies": compare_license_counts(license_data),
        "special_licenses": validate_special_licenses(license_data),
        "total_counts": validate_total_counts(license_data),
    }
    
    total_warnings = sum(len(w) for w in checks.values())
    
    if total_warnings == 0:
        LOGGER.info("✓ All sanity checks passed!")
    else:
        LOGGER.warning(
            f"Found {total_warnings} warning(s) across sanity checks"
        )
    
    return checks


def print_summary_report(summary, sanity_checks=None, license_data=None):
    """
    Print a comprehensive summary report of the query operation.
    
    Args:
        summary: Dictionary with summary statistics
        sanity_checks: Optional dict with sanity check results
        license_data: Optional list of license data for detailed stats
    """
    LOGGER.info("")
    LOGGER.info("=" * 80)
    LOGGER.info("EXECUTION SUMMARY REPORT")
    LOGGER.info("=" * 80)
    
    # Processing Statistics
    LOGGER.info("PROCESSING STATISTICS")
    LOGGER.info("-" * 80)
    LOGGER.info(f"Total licenses in dataset: {len(CC_LICENSE_CATEGORIES)}")
    LOGGER.info(f"Licenses processed: {summary['total']}")
    LOGGER.info(f"  ✓ Successful: {summary['success']}")
    LOGGER.info(f"  ✗ Failed: {summary['failure']}")
    if summary.get('skipped', 0) > 0:
        LOGGER.info(f"  ⊘ Skipped: {summary['skipped']}")
    
    if summary['failure'] > 0:
        success_rate = (summary['success'] / summary['total']) * 100
        LOGGER.warning(f"Success rate: {success_rate:.1f}%")
        LOGGER.warning("Failed licenses:")
        for license_name, error in summary['failed_licenses']:
            LOGGER.warning(f"  - {license_name}: {error}")
    else:
        LOGGER.info("Success rate: 100.0%")
    
    if summary.get('skipped_licenses'):
        LOGGER.warning("Skipped licenses (category not found):")
        for license_name in summary['skipped_licenses']:
            LOGGER.warning(f"  - {license_name}")
    
    # Data Statistics
    LOGGER.info("")
    LOGGER.info("DATA STATISTICS")
    LOGGER.info("-" * 80)
    total_files = summary.get('total_files', 0)
    total_pages = summary.get('total_pages', 0)
    LOGGER.info(f"Total files counted: {total_files:,}")
    LOGGER.info(f"Total pages counted: {total_pages:,}")
    
    if license_data:
        # Find licenses with suspiciously low counts
        low_count_licenses = []
        for item in license_data:
            file_count = item.get("FILE_COUNT", 0)
            license_name = item.get("LICENSE", "Unknown")
            # Flag licenses with less than 100 files (except old versions)
            version = extract_license_version(license_name)
            if file_count < 100 and (not version or version >= 3.0):
                low_count_licenses.append((license_name, file_count))
        
        if low_count_licenses:
            LOGGER.warning("")
            LOGGER.warning("Licenses with suspiciously low counts (< 100 files):")
            for license_name, count in sorted(
                low_count_licenses, key=lambda x: x[1]
            ):
                LOGGER.warning(f"  - {license_name}: {count:,} files")
    
    # Performance Statistics
    LOGGER.info("")
    LOGGER.info("PERFORMANCE STATISTICS")
    LOGGER.info("-" * 80)
    LOGGER.info(f"Total execution time: {format_time_remaining(summary['total_time'])}")
    LOGGER.info(
        f"Average time per license: "
        f"{summary['avg_time_per_license']:.1f}s"
    )
    if summary['total'] > 0:
        licenses_per_minute = (summary['total'] / summary['total_time']) * 60
        LOGGER.info(f"Processing rate: {licenses_per_minute:.2f} licenses/min")
    
    # Cache Statistics
    if 'cache_size' in summary:
        LOGGER.info("")
        LOGGER.info("CACHE STATISTICS")
        LOGGER.info("-" * 80)
        LOGGER.info(f"Categories cached: {summary['cache_size']}")
        if 'cache_hits' in summary and 'cache_misses' in summary:
            total_cache_requests = (
                summary['cache_hits'] + summary['cache_misses']
            )
            LOGGER.info(f"Cache hits: {summary['cache_hits']:,}")
            LOGGER.info(f"Cache misses: {summary['cache_misses']:,}")
            if total_cache_requests > 0:
                LOGGER.info(
                    f"Cache hit rate: {summary.get('cache_hit_rate', 0):.1f}%"
                )
    
    # Sanity Checks & Validation
    if sanity_checks:
        LOGGER.info("")
        LOGGER.info("DATA QUALITY & VALIDATION")
        LOGGER.info("-" * 80)
        
        total_warnings = sum(len(w) for w in sanity_checks.values())
        
        if total_warnings == 0:
            LOGGER.info("✓ All sanity checks passed!")
        else:
            LOGGER.warning(f"Found {total_warnings} warning(s) across sanity checks:")
            
            if sanity_checks.get("popular_licenses"):
                LOGGER.warning("")
                LOGGER.warning("Popular License Checks:")
                for warning in sanity_checks["popular_licenses"]:
                    LOGGER.warning(f"  {warning}")
            
            if sanity_checks.get("version_progression"):
                LOGGER.warning("")
                LOGGER.warning("Version Progression Issues:")
                for warning in sanity_checks["version_progression"]:
                    LOGGER.warning(f"  {warning}")
            
            if sanity_checks.get("count_anomalies"):
                LOGGER.warning("")
                LOGGER.warning("Count Anomalies:")
                for warning in sanity_checks["count_anomalies"]:
                    LOGGER.warning(f"  {warning}")
            
            if sanity_checks.get("special_licenses"):
                LOGGER.warning("")
                LOGGER.warning("Special License Issues:")
                for warning in sanity_checks["special_licenses"]:
                    LOGGER.warning(f"  {warning}")
            
            if sanity_checks.get("total_counts"):
                LOGGER.warning("")
                LOGGER.warning("Total Count Issues:")
                for warning in sanity_checks["total_counts"]:
                    LOGGER.warning(f"  {warning}")
    
    LOGGER.info("=" * 80)


def main():
    """Main function to orchestrate the WikiCommons data fetch."""
    script_start_time = time.time()
    exit_code = 0
    
    try:
        args = parse_arguments()
        
        # Print startup banner
        print_startup_banner(args)
        
        shared.paths_log(LOGGER, PATHS)
        
        # Set debug logging level if requested
        if args.debug:
            import logging
            LOGGER.setLevel(logging.DEBUG)
            LOGGER.debug("Debug logging enabled")
        
        # Test category conversion before proceeding
        test_category_conversion()
        
        # Handle dry-run mode
        if args.dry_run:
            LOGGER.info("DRY RUN MODE: Testing conversions and validation only")
            session = get_requests_session(
                request_timeout=args.timeout,
                debug=args.debug
            )
            
            # Validate categories exist
            if not args.skip_validation:
                valid_categories = validate_categories_exist(session)
                LOGGER.info(
                    f"Dry run complete: {len(valid_categories)} categories "
                    "validated"
                )
            else:
                LOGGER.info("Dry run complete: Skipped validation")
            
            total_time = time.time() - script_start_time
            LOGGER.info("")
            LOGGER.info(f"Dry run completed in {format_time_remaining(total_time)}")
            return 0
        
        check_for_completion()
        
        session = get_requests_session(
            request_timeout=args.timeout,
            debug=args.debug
        )
        
        # Validate categories exist before expensive recursive queries
        if args.skip_validation:
            LOGGER.info("Skipping category validation (--skip-validation)")
            # Create valid_categories from all licenses without validation
            valid_categories = [
                (license_name, license_to_wikicommons_category(license_name))
                for license_name in CC_LICENSE_CATEGORIES
            ]
        else:
            valid_categories = validate_categories_exist(session)
        
        license_data, summary = query_wikicommons(
            args, session, valid_categories
        )
        
        # Perform sanity checks and validations
        sanity_checks = perform_sanity_checks(license_data)
        
        # Print comprehensive summary report
        print_summary_report(summary, sanity_checks, license_data)
        
        # Write data to file
        args = write_data(args, license_data)
        
        # Git operations
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add and commit new WikiCommons data for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])
        
        # Calculate total execution time
        total_execution_time = time.time() - script_start_time
        
        # Print final success message
        LOGGER.info("")
        LOGGER.info("=" * 80)
        LOGGER.info("EXECUTION COMPLETED SUCCESSFULLY")
        LOGGER.info("=" * 80)
        LOGGER.info(f"Total execution time: {format_time_remaining(total_execution_time)}")
        
        if args.enable_save:
            LOGGER.info(f"Output file: {FILE1_COUNT}")
            LOGGER.info(f"Total records written: {len(license_data)}")
        
        # Determine exit code based on results
        if summary['failure'] > 0:
            exit_code = 1
            LOGGER.warning(
                f"Completed with {summary['failure']} failure(s). "
                "Please review the summary report above."
            )
        else:
            total_warnings = (
                sum(len(w) for w in sanity_checks.values())
                if sanity_checks else 0
            )
            if total_warnings > 0:
                exit_code = 0  # Warnings don't fail the script
                LOGGER.info(
                    f"Completed with {total_warnings} warning(s). "
                    "Data quality checks completed."
                )
            else:
                LOGGER.info("✓ All operations completed successfully!")
                LOGGER.info("✓ All data quality checks passed!")
        
        LOGGER.info("")
        LOGGER.info("Next steps:")
        LOGGER.info("  1. Review the output file for data accuracy")
        LOGGER.info("  2. Check the summary report for any warnings")
        if args.enable_save:
            LOGGER.info(f"  3. Process the data file: {FILE1_COUNT}")
        LOGGER.info("=" * 80)
        
        return exit_code
        
    except shared.QuantifyingException as e:
        total_execution_time = time.time() - script_start_time
        LOGGER.error("")
        LOGGER.error("=" * 80)
        LOGGER.error("EXECUTION FAILED")
        LOGGER.error("=" * 80)
        LOGGER.error(f"Error: {e.message}")
        LOGGER.error(f"Execution time: {format_time_remaining(total_execution_time)}")
        LOGGER.error("=" * 80)
        return e.exit_code
    except Exception as e:
        total_execution_time = time.time() - script_start_time
        LOGGER.critical("")
        LOGGER.critical("=" * 80)
        LOGGER.critical("UNEXPECTED ERROR")
        LOGGER.critical("=" * 80)
        LOGGER.critical(f"Error: {str(e)}")
        LOGGER.critical(f"Execution time: {format_time_remaining(total_execution_time)}")
        LOGGER.critical("=" * 80)
        raise


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        if e.code != 0:
            LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code if e.code is not None else 0)
    except KeyboardInterrupt:
        LOGGER.info("")
        LOGGER.info("=" * 80)
        LOGGER.warning("Execution interrupted by user (KeyboardInterrupt)")
        LOGGER.info("=" * 80)
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
        LOGGER.critical("")
        LOGGER.critical("=" * 80)
        LOGGER.critical("UNHANDLED EXCEPTION")
        LOGGER.critical("=" * 80)
        LOGGER.critical(f"Traceback:\n{traceback_formatted}")
        LOGGER.critical("=" * 80)
        sys.exit(1)
