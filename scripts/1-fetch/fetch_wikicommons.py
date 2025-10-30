#!/usr/bin/env python
"""
Fetch CC Legal Tool usage data from WikiCommons API.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
import urllib.parse

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
BASE_URL = "https://commons.wikimedia.org/w/api.php"
# Creative Commons license categories to query
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
FILE1_COUNT = shared.path_join(PATHS["data_phase"], "wikicommons_1_count.csv")
HEADER1_COUNT = ["LICENSE", "FILE_COUNT", "PAGE_COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

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
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def check_for_completion():
    """Check if data fetch is already completed for this quarter."""
    try:
        with open(FILE1_COUNT, "r", newline="") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            if len(list(reader)) >= len(CC_LICENSE_CATEGORIES):
                raise shared.QuantifyingException(
                    f"Data fetch completed for {QUARTER}", 0
                )
    except FileNotFoundError:
        pass  # File may not be found without --enable-save, etc.


def get_requests_session():
    """Create a requests session with retry logic."""
    max_retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=shared.RETRY_STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    headers = {
        "User-Agent": shared.USER_AGENT
    }
    session.headers.update(headers)
    return session


def get_category_info(session, category_name):
    """
    Get file and page count for a specific category.
    
    Args:
        session: requests.Session object
        category_name: Name of the category to query
        
    Returns:
        dict: Dictionary with 'files' and 'pages' counts
    """
    params = {
        "action": "query",
        "prop": "categoryinfo",
        "titles": f"Category:{category_name}",
        "format": "json"
    }
    
    try:
        with session.get(BASE_URL, params=params) as response:
            response.raise_for_status()
            data = response.json()
            
        pages = data.get("query", {}).get("pages", {})
        if not pages:
            LOGGER.warning(f"No data found for category: {category_name}")
            return None
            
        # Get the first (and usually only) page result
        page_data = list(pages.values())[0]
        categoryinfo = page_data.get("categoryinfo", {})
        
        files = categoryinfo.get("files", 0)
        pages = categoryinfo.get("pages", 0)
        
        LOGGER.info(f"Category {category_name}: {files} files, {pages} pages")
        return {"files": files, "pages": pages}
        
    except requests.HTTPError as e:
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        raise shared.QuantifyingException(f"KeyError: {e}", 1)


def get_subcategories(session, category_name):
    """
    Get subcategories for a given category.
    
    Args:
        session: requests.Session object
        category_name: Name of the parent category
        
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
    
    try:
        with session.get(BASE_URL, params=params) as response:
            response.raise_for_status()
            data = response.json()
            
        members = data.get("query", {}).get("categorymembers", [])
        subcategories = []
        
        for member in members:
            title = member.get("title", "")
            if title.startswith("Category:"):
                subcat_name = title.replace("Category:", "")
                subcategories.append(subcat_name)
                
        LOGGER.info(f"Found {len(subcategories)} subcategories for {category_name}")
        return subcategories
        
    except requests.HTTPError as e:
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        raise shared.QuantifyingException(f"KeyError: {e}", 1)


def recursively_count_category(session, category_name, visited=None):
    """
    Recursively count files and pages in a category and its subcategories.
    
    Args:
        session: requests.Session object
        category_name: Name of the category to count
        visited: Set of already visited categories (for cycle detection)
        
    Returns:
        dict: Dictionary with 'files' and 'pages' counts
    """
    if visited is None:
        visited = set()
        
    if category_name in visited:
        LOGGER.warning(f"Cycle detected for category: {category_name}")
        return {"files": 0, "pages": 0}
        
    visited.add(category_name)
    
    # Get direct counts for this category
    counts = get_category_info(session, category_name)
    
    # Get subcategories and recursively count them
    subcategories = get_subcategories(session, category_name)
    
    for subcat in subcategories:
        if subcat not in visited:  # Avoid infinite recursion
            subcat_counts = recursively_count_category(session, subcat, visited.copy())
            counts["files"] += subcat_counts["files"]
            counts["pages"] += subcat_counts["pages"]
    
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

    with open(FILE1_COUNT, "w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER1_COUNT, dialect="unix"
        )
        writer.writeheader()
        for row in license_data:
            writer.writerow(row)
    
    LOGGER.info(f"Data written to {FILE1_COUNT}")
    return args


def query_wikicommons(args, session):
    """Query WikiCommons API for CC license data."""
    license_data = []
    
    for category in CC_LICENSE_CATEGORIES:
        LOGGER.info(f"Processing category: {category}")

            counts = recursively_count_category(session, category)
            license_data.append({
                "LICENSE": category,
                "FILE_COUNT": counts["files"],
                "PAGE_COUNT": counts["pages"]
            })
    
    return license_data


def main():
    """Main function to orchestrate the WikiCommons data fetch."""
    LOGGER.info("Script execution started.")
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    check_for_completion()
    
    session = get_requests_session()
    license_data = query_wikicommons(args, session)
    args = write_data(args, license_data)
    
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new WikiCommons data for {QUARTER}",
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
