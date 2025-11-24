#!/usr/bin/env python
"""
Fetch high-level WikiCommons statistics for Quantifying the Commons.
Generates one dataset:
1) Recursive category data (aggregated by LICENSE TYPE, File Count, Page Count)
Uses Wikimedia Commons API to retrieve metadata
for Creative Commons license categories.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback

# Third-party
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory for shared imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
BASE_URL = "https://commons.wikimedia.org/w/api.php"
FILE_WIKICOMMONS = shared.path_join(
    PATHS["data_phase"], "wikicommons_legal_tool_counts.csv"
)
HEADER_WIKICOMMONS = ["LICENSE_TYPE", "FILE_COUNT", "PAGE_COUNT"]
ROOT_CATEGORY = "Free_Creative_Commons_licenses"
TIMEOUT = 25


def parse_arguments():
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results to CSV.",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, push).",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit recursive depth for testing",
    )

    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_subcategories(category, session):
    """Fetch all subcategories for a
    given category, handling pagination"""
    all_subcats = []
    cmcontinue = None

    while True:
        try:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{category}",
                "cmtype": "subcat",
                "format": "json",
                "cmlimit": "max",
            }
            if cmcontinue:
                params["cmcontinue"] = cmcontinue

            resp = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            members = data.get("query", {}).get("categorymembers", [])
            subcats = [
                m["title"].replace("Category:", "").replace("&", "%26")
                for m in members
            ]
            all_subcats.extend(subcats)

            # Handle pagination
            if "continue" in data and "cmcontinue" in data["continue"]:
                cmcontinue = data["continue"]["cmcontinue"]
                time.sleep(0.2)
            else:
                break

        except Exception as e:
            LOGGER.warning(
                f"Failed to fetch subcategories for {category}: {e}"
            )
            break

    return all_subcats


def fetch_category_totals(category, session):
    """Fetch total file and page counts for a category."""
    try:
        params = {
            "action": "query",
            "prop": "categoryinfo",
            "titles": f"Category:{category}",
            "format": "json",
        }
        resp = session.get(BASE_URL, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        file_cnt, page_cnt = 0, 0
        for _, info in data.get("query", {}).get("pages", {}).items():
            catinfo = info.get("categoryinfo", {})
            file_cnt += catinfo.get("files", 0)
            page_cnt += catinfo.get("pages", 0)
        return {"FILE_COUNT": file_cnt, "PAGE_COUNT": page_cnt}
    except Exception as e:
        message = f"Failed to fetch contents for {category}: {e}"
        LOGGER.error(message)
        raise shared.QuantifyingException(message)


# Helper function to check if a category
# name represents a valid CC license tool
def is_valid_license_tool(category_name):
    """
    Checks if a category name corresponds to
    an official Creative Commons license tool..
    Official license categories usually start with
    'CC-' followed by a combination
    of BY, SA, ND, NC, and a version number (e.g., CC-BY-4.0)

    EXCLUDED CC Licenses (marked 'Not OK' in policy):
    - Attribution-NonCommercial (CC BY-NC).
    - Attribution-NoDerivs (CC BY-ND).
    - Any combination containing NC or ND restrictions.


    """
    # A list of common patterns to check
    if category_name.startswith("CC-") and any(
        x in category_name for x in ["BY", "SA"]
    ):
        # Specific exceptions that look like
        # licenses but are markers/subcategories
        if "migrated" in category_name or "Retired" in category_name:
            return False
        return True

    # Check for CC0 Public Domain Dedication (often just "CC0")
    if (
        category_name == "CC0"
        or category_name.startswith("CC0-")
        or category_name == "CC-Zero"
    ):
        return True

    # The root category itself is not a license tool
    if category_name == ROOT_CATEGORY:
        return False

    return False


def recursive_collect_data(session, limit=None):
    """Recursively traverse WikiCommons categories and collect data."""

    results = []
    visited = set()

    def traverse(category, path, depth=0):
        if limit and depth >= limit:
            return
        if category in visited:
            return
        visited.add(category)

        # Only fetch and collect data for valid license tools
        if is_valid_license_tool(category):
            try:
                # Get counts for the current category
                contents = fetch_category_totals(category, session)

                results.append(
                    {
                        # Use the specific license category name
                        #  as the LICENSE_TYPE
                        "LICENSE_TYPE": category,
                        "FILE_COUNT": contents["FILE_COUNT"],
                        "PAGE_COUNT": contents["PAGE_COUNT"],
                    }
                )
            except shared.QuantifyingException as e:
                # Log the specific license category failure
                LOGGER.error(
                    f"Failed to process valid license category {category}: {e}"
                )

        # Get subcategories (check subcategories,
        # as a valid license might be nested under a non-license category)
        subcats = get_subcategories(category, session)

        # Logging label
        label = "categories" if depth == 0 else "subcategories"
        LOGGER.info(f"Fetched {len(subcats)} {label} for {category}.")

        # Recursively traverse subcategories
        for sub in subcats:
            # Use the subcategory name as the 'path' for traversal,
            # but use the category name for the final result.
            traverse(sub, f"{path}/{sub}", depth + 1)
            time.sleep(0.05)  # time to sleep

    # Start traversal from root
    traverse(ROOT_CATEGORY, ROOT_CATEGORY)
    return results


def write_data(args, wikicommons_data):
    """Write WikiCommons data to CSV."""
    if not args.enable_save:
        return args

    os.makedirs(PATHS["data_phase"], exist_ok=True)
    with open(
        FILE_WIKICOMMONS, "w", encoding="utf-8", newline="\n"
    ) as file_obj:

        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_WIKICOMMONS, dialect="unix"
        )
        writer.writeheader()
        writer.writerows(wikicommons_data)

    LOGGER.info(f"Saved {len(wikicommons_data)} rows to {FILE_WIKICOMMONS}.")
    return args


def main():
    args = parse_arguments()
    LOGGER.info("Starting WikiCommons data fetch.")
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    session = shared.get_session(accept_header="application/json")
    wikicommons_data = recursive_collect_data(session, limit=args.limit)
    args = write_data(args, wikicommons_data)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        "Add WikiCommons dataset for Quantifying the Commons.",
    )
    shared.git_push_changes(args, PATHS["repo"])

    LOGGER.info("WikiCommons fetch completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
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
