#!/usr/bin/env python
"""
Fetch CC Legal Tool usage from Wikipedia API.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
from operator import itemgetter

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
FILE_LANGUAGES = os.path.join(
    PATHS["data_phase"], "wikipedia_count_by_languages.csv"
)
HEADER_LANGUAGES = [
    "LANGUAGE_CODE",
    "LANGUAGE_NAME_EN",
    "LANGUAGE_NAME",
    "COUNT",
]
QUARTER = os.path.basename(PATHS["data_quarter"])
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_MATRIX_URL = "https://meta.wikimedia.org/w/api.php"


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


def get_requests_session():
    max_retries = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update({"User-Agent": shared.USER_AGENT})
    return session


def write_data(args, tool_data):
    if not args.enable_save:
        return args
    LOGGER.info("Saving fetched data")
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    with open(FILE_LANGUAGES, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_LANGUAGES, dialect="unix"
        )
        writer.writeheader()
        for row in tool_data:
            writer.writerow(row)
    return args


def query_wikipedia_languages(session):
    LOGGER.info("Fetching article counts from all language Wikipedias")
    tool_data = []

    # Gets all language wikipedias
    params = {"action": "sitematrix", "format": "json", "uselang": "en"}
    r = session.get(WIKIPEDIA_MATRIX_URL, params=params, timeout=30)
    data = r.json()["sitematrix"]

    languages = []
    for key, val in data.items():
        if not isinstance(val, dict):
            continue
        if key.isdigit():
            language_code = val.get("code")
            language_name = val.get("name")
            language_name_en = val.get("localname")
        for site in val.get("site", []):
            if "wikipedia.org" in site["url"]:
                languages.append(
                    {
                        "code": language_code,
                        "name": language_name,
                        "name_en": language_name_en,
                        "url": site["url"],
                    }
                )
    languages = sorted(languages, key=itemgetter("code", "name_en"))
    # For each language wikipedia, fetch statistics.
    for site in languages:
        base_url = f"{site['url']}/w/api.php"
        params = {
            "action": "query",
            "meta": "siteinfo",
            "siprop": "statistics",
            "format": "json",
        }
        try:
            r = session.get(base_url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            stats = data["query"]["statistics"]
            article_count = stats.get("articles", 0)
            language_code = site["code"]
            language_name = site["name"]
            language_name_en = site["name_en"]

            language_display = f"{language_code}"
            if language_name_en:
                language_display = f"{language_display} {language_name_en}"
            if language_name:
                language_display = f"{language_display} ({language_name})"

            if article_count == 0:
                LOGGER.warning(f"Skipping {language_display} with 0 articles")
                continue
            tool_data.append(
                {
                    "LANGUAGE_CODE": language_code,
                    "LANGUAGE_NAME_EN": language_name_en,
                    "LANGUAGE_NAME": language_name,
                    "COUNT": article_count,
                }
            )
            LOGGER.info(f"{language_display}: {article_count}")

        except Exception as e:
            LOGGER.warning(f"Failed to fetch for {language_display}): {e}")

    tool_data = sorted(
        tool_data, key=itemgetter("LANGUAGE_CODE", "LANGUAGE_NAME_EN")
    )
    return tool_data


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    tool_data = query_wikipedia_languages(get_requests_session())
    args = write_data(args, tool_data)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Wikipedia data for {QUARTER}",
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
