#!/usr/bin/env python
"""
Fetch Creative Commons related statistics from Wikipedia.
"""

# Standard library
import argparse
import csv
import os
import sys
from typing import Dict

# Third-party
import requests

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
# First-party / Local
import shared  # noqa: E402

# -------------------- Setup Logger and Paths --------------------
# LOGGER: used for logging info, warnings, errors
# PATHS: contains standard paths for data, repo, dotenv, etc.
LOGGER, PATHS = shared.setup(__file__)

# -------------------- Constants --------------------
WIKI_API = "https://en.wikipedia.org/w/api.php"

# Add User-Agent to avoid 403 errors
HEADERS = {
    "User-Agent": (
        "QuantifyingCommonsBot/1.0 "
        "(https://github.com/YOUR_USERNAME/quantifying)"
    )
}

# CSV file to store Creative Commons statistics
FILE_CC_STATS = shared.path_join(PATHS["data_phase"], "wikipedia_cc_stats.csv")
HEADER_CC_STATS = ["LICENSE", "COUNT", "SAMPLE_TITLES"]

# -------------------- Functions --------------------


def get_site_statistics() -> Dict[str, int]:
    """
    Fetch general statistics from Wikipedia.

    Returns:
        dict: Dictionary containing:
            - articles: number of articles
            - pages: number of pages
            - edits: number of edits
            - users: number of users
            - images: number of images
    """
    params = {
        "action": "query",
        "meta": "siteinfo",
        "siprop": "statistics|rightsinfo",
        "format": "json",
    }

    try:
        response = requests.get(WIKI_API, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        LOGGER.error(f"Error fetching site statistics: {e}")
        return {}

    stats = data.get("query", {}).get("statistics", {})
    return {
        "articles": stats.get("articles", 0),
        "pages": stats.get("pages", 0),
        "edits": stats.get("edits", 0),
        "users": stats.get("users", 0),
        "images": stats.get("images", 0),
    }


def search_articles_by_license(
    license_keyword: str, limit: int = 10
) -> Dict[str, int]:
    """
    Search Wikipedia articles containing a specific Creative Commons license.

    Args:
        license_keyword (str): e.g., "CC BY-SA 4.0"
        limit (int): Max number of results to fetch

    Returns:
        dict: Dictionary with count and sample article titles
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": license_keyword,
        "srlimit": limit,
        "format": "json",
    }

    try:
        response = requests.get(WIKI_API, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        LOGGER.error(f"Error searching articles for '{license_keyword}': {e}")
        return {"count": 0, "sample_titles": []}

    search_results = data.get("query", {}).get("search", [])
    return {
        "count": len(search_results),
        "sample_titles": [item["title"] for item in search_results],
    }


def initialize_data_file(file_path: str, header: list):
    """
    Initialize CSV file with header if it does not exist.
    """
    if not os.path.isfile(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header, dialect="unix")
            writer.writeheader()
        LOGGER.info(f"Created new CSV file: {file_path}")


def save_license_results(license_keyword: str, results: dict, args):
    """
    Save Creative Commons search results to CSV.
    """
    if not args.enable_save:
        return

    with open(FILE_CC_STATS, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER_CC_STATS, dialect="unix")
        writer.writerow(
            {
                "LICENSE": license_keyword,
                "COUNT": results["count"],
                "SAMPLE_TITLES": "; ".join(results["sample_titles"]),
            }
        )
    LOGGER.info(f"Saved results for '{license_keyword}' to CSV.")


def parse_arguments():
    """
    Parse command-line arguments for the fetch script.
    """
    parser = argparse.ArgumentParser(description="Fetch Wikipedia CC data")
    parser.add_argument(
        "--limit", type=int, default=10, help="Max number of results to fetch"
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results to CSV",
    )
    parser.add_argument(
        "--enable-git", action="store_true", help="Enable git commit/push"
    )
    return parser.parse_args()


# -------------------- Main Execution --------------------
def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)  # Log paths

    # Ensure CSV exists
    initialize_data_file(FILE_CC_STATS, HEADER_CC_STATS)

    # Fetch site statistics
    LOGGER.info("Fetching Wikipedia site statistics")
    stats = get_site_statistics()
    LOGGER.info(f"Site statistics: {stats}")

    # Fetch articles mentioning Creative Commons
    license_query = "Creative Commons"
    results = search_articles_by_license(license_query, limit=args.limit)
    save_license_results(license_query, results, args)
    LOGGER.info(f"Fetched {results['count']} articles for '{license_query}'")

    # Optional: commit and push via git if enabled
    if args.enable_git:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            "Add Wikipedia CC data",
        )
        shared.git_push_changes(args, PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOGGER.error(f"Unhandled exception: {e}")
        sys.exit(1)
