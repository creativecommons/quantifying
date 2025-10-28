#!/usr/bin/env python
"""
Fetch high-level Europeana statistics for Quantifying the Commons.
Generates two datasets:
1) Without themes (aggregated by DATA_PROVIDER, LEGAL_TOOL)
2) With all themes (aggregated by DATA_PROVIDER, LEGAL_TOOL, THEME)
Uses totalResults instead of looping through pages for efficiency.
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
import requests
from dotenv import load_dotenv
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from requests.adapters import HTTPAdapter, Retry

# Add parent directory for shared imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)
load_dotenv(PATHS["dotenv"])

# Constants
EUROPEANA_API_KEY = os.getenv("EUROPEANA_API_KEY")
BASE_URL = "https://api.europeana.eu/record/v2/search.json"
FILE_WITH_THEMES = shared.path_join(
    PATHS["data_phase"], "europeana_with_themes.csv"
)
FILE_WITHOUT_THEMES = shared.path_join(
    PATHS["data_phase"], "europeana_without_themes.csv"
)
HEADER_WITH_THEMES = ["DATA_PROVIDER", "LEGAL_TOOL", "THEME", "COUNT"]
HEADER_WITHOUT_THEMES = ["DATA_PROVIDER", "LEGAL_TOOL", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])
# Define themes here (alphabetically for consistency)
# Themes are listed at
# https://europeana.atlassian.net/wiki/spaces/EF/pages/2385739812/Search+API+Documentation#Request
# (in the Search API Request Parameter accordion)

THEMES = [
    "archaeology",
    "art",
    "fashion",
    "industrial",
    "manuscript",
    "map",
    "migration",
    "music",
    "nature",
    "newspaper",
    "photography",
    "sport",
    "ww1",
]

RIGHTS_LABEL_MAP = {
    "InC": "In Copyright",
    "InC-EDU": "In Copyright – Educational Use Only",
    "InC-OW-EU": "In Copyright – EU Only",
    "CNE": "No Copyright – Contractual Restrictions",
    "NoC-NC": "No Copyright – Non-Commercial Use Only",
    "NoC-OKLR": "No Copyright – Other Known Legal Restrictions",
}


def parse_arguments():
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, push)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Limit number of providers for testing.",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_requests_session():
    """Create a requests session with retry."""
    max_retries = Retry(
        total=5, backoff_factor=5, status_forcelist=shared.STATUS_FORCELIST
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=max_retries))
    session.headers.update(
        {"accept": "application/json", "User-Agent": shared.USER_AGENT}
    )
    return session


def simplify_legal_tool(legal_tool):
    """Simplify license URLs into human-readable labels


    This function converts long or complex license URLs into
    short, human-readable labels such as:
      - "CC BY-SA 4.0"
      - "CC BY-NC-ND 3.0 AT"
      - "Public Domain (CC0 1.0)"
      - "Public Domain (PDM 1.0)"
      - "Rights Statement: In copyright"

    It handles:
    - Non-ported Creative Commons licenses (no jurisdiction)
    - Ported Creative Commons licenses (with jurisdiction codes)
    - Public Domain identifiers (CC0, PDM)
    - RightsStatements.org URLs
    - Other license URLs (returns simplified or original form)

    Examples
    --------
     simplify_legal_tool("http://creativecommons.org/licenses/by-sa/4.0/")
    'CC BY-SA 4.0'

     simplify_legal_tool("http://creativecommons.org/licenses/by-nc-nd/3.0/at/")
    'CC BY-NC-ND 3.0 AT'

     simplify_legal_tool("http://rightsstatements.org/vocab/InC/1.0/")
    'Rights Statement: In copyright'

    simplify_legal_tool("http://creativecommons.org/publicdomain/zero/1.0/")
    'Public Domain (CC0 1.0)'

    simplify_legal_tool("http://creativecommons.org/publicdomain/mark/1.0/")
    'Public Domain (PDM 1.0)'

    simplify_legal_tool("Public Domain")
    'Public Domain'

    Parameters
    ----------
    legal_tool : str
        A license string or URL, e.g., a Creative Commons license,
        a RightsStatement.org URL,
        or a Public Domain identifier.

    Returns
    -------
    str
        A short, standardized form of the license.
    """

    if not isinstance(legal_tool, str):
        return legal_tool
    if not legal_tool.startswith("http"):
        return legal_tool

    # Public domain handling
    if "publicdomain" in legal_tool:
        if "zero" in legal_tool.lower():
            return "Public Domain (CC0 1.0)"
        if "mark" in legal_tool.lower():
            return "Public Domain (PDM 1.0)"
        return "Public Domain"

    # RightsStatements.org handling
    if "rightsstatements.org" in legal_tool:
        parts = legal_tool.strip("/").split("/")
        code = parts[-2]
        if code in RIGHTS_LABEL_MAP:
            return f"Rights Statement: {RIGHTS_LABEL_MAP[code]}"
        return f"Rights Statement: {code}"

    # Creative Commons handling
    if "creativecommons.org" in legal_tool:
        parts = legal_tool.strip("/").split("/")
        last_parts = parts[-3:]
        jurisdiction = ""
        if len(last_parts[-1]) == 2 and last_parts[-1].isalpha():
            jurisdiction = last_parts[-1].upper()
            last_parts = last_parts[:-1]
        joined = " ".join(
            part.upper() for part in last_parts if part and part != "licenses"
        )
        return f"CC {joined} {jurisdiction}".strip()

    return legal_tool


def get_facet_list(session, facet_field):
    """Fetch complete facet list from Europeana API for a given facet field."""
    all_values = []
    offset = 0
    limit = 1000

    LOGGER.info(f"Fetching {facet_field} facet values.")

    while True:
        params = {
            "wskey": EUROPEANA_API_KEY,
            "query": "*",
            "rows": 0,
            "facet": facet_field,
            f"f.{facet_field}.facet.limit": limit,
            f"f.{facet_field}.facet.offset": offset,
            "profile": "facets",
        }

        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            LOGGER.warning(
                f"Failed fetching facet {facet_field} at offset {offset}: {e}"
            )
            break

        facets = data.get("facets", [])
        if not facets or not facets[0].get("fields"):
            break

        fields = facets[0]["fields"]
        new_values = [f["label"] for f in fields if f.get("label")]

        for v in new_values:
            if v not in all_values:
                all_values.append(v)

        if len(new_values) < limit:
            break

        offset += limit
        time.sleep(0.5)

    LOGGER.info(
        f"Completed fetching {facet_field}. Total unique: {len(all_values)}"
    )
    all_values.sort()
    return all_values


def fetch_europeana_data_without_themes(session, limit=None):
    """Fetch counts by DATA_PROVIDER and RIGHTS using facets."""
    LOGGER.info("Fetching Europeana counts without themes.")

    params = {
        "wskey": EUROPEANA_API_KEY,
        "query": "*",
        "rows": 0,
        "profile": "facets",
        "facet": ["DATA_PROVIDER", "RIGHTS"],
        "f.DATA_PROVIDER.facet.limit": 1000,
        "f.RIGHTS.facet.limit": 100,
    }

    try:
        resp = session.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        LOGGER.error(f"Failed to fetch facets: {e}")
        return []

    facets = {f["name"]: f["fields"] for f in data.get("facets", [])}
    provider_fields = facets.get("DATA_PROVIDER", [])
    rights_fields = facets.get("RIGHTS", [])
    if limit:
        provider_fields = provider_fields[:limit]

    output = []
    for provider_entry in provider_fields:
        provider = provider_entry["label"]
        provider_count = provider_entry["count"]
        if provider_count == 0:
            continue
        LOGGER.info(f"Fetching rights data for provider={provider}")
        for rights_entry in rights_fields:
            rights = rights_entry["label"]
            query = f'DATA_PROVIDER:"{provider}" AND RIGHTS:"{rights}"'
            params_detail = {
                "wskey": EUROPEANA_API_KEY,
                "rows": 0,
                "query": query,
            }
            try:
                resp_detail = session.get(
                    BASE_URL, params=params_detail, timeout=60
                )
                resp_detail.raise_for_status()
                count = resp_detail.json().get("totalResults", 0)
                if count > 0:
                    output.append(
                        {
                            "DATA_PROVIDER": provider,
                            "LEGAL_TOOL": simplify_legal_tool(rights),
                            "COUNT": count,
                        }
                    )

            except requests.RequestException as e:
                LOGGER.warning(
                    f"Failed for provider={provider}, rights={rights}: {e}"
                )
            time.sleep(0.2)
    LOGGER.info(f"Aggregated {len(output)} records (without themes).")
    return output


def fetch_europeana_data_with_themes(session, themes, limit=None):
    """Fetch counts by DATA_PROVIDER, RIGHTS, and THEME using facets."""
    LOGGER.info("Fetching Europeana counts with themes")

    params = {
        "wskey": EUROPEANA_API_KEY,
        "query": "*",
        "rows": 0,
        "profile": "facets",
        "facet": ["DATA_PROVIDER", "RIGHTS"],
        "f.DATA_PROVIDER.facet.limit": 1000,
        "f.RIGHTS.facet.limit": 100,
    }

    try:
        resp = session.get(BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        LOGGER.error(f"Failed to fetch facets: {e}")
        return []

    facets = {f["name"]: f["fields"] for f in data.get("facets", [])}
    provider_fields = facets.get("DATA_PROVIDER", [])
    rights_fields = facets.get("RIGHTS", [])
    if limit:
        provider_fields = provider_fields[:limit]

    output = []
    for provider_entry in provider_fields:
        provider = provider_entry["label"]
        provider_count = provider_entry["count"]
        if provider_count == 0:
            continue
        LOGGER.info(f"Fetching theme+rights data for provider={provider}")
        for rights_entry in rights_fields:
            rights = rights_entry["label"]
            simplified_rights = simplify_legal_tool(rights)

            for theme in themes:
                query = f'DATA_PROVIDER:"{provider}" AND RIGHTS:"{rights}"'
                params_detail = {
                    "wskey": EUROPEANA_API_KEY,
                    "rows": 0,
                    "query": query,
                    "theme": theme,
                }
                try:
                    resp_detail = session.get(
                        BASE_URL, params=params_detail, timeout=30
                    )
                    resp_detail.raise_for_status()
                    count = resp_detail.json().get("totalResults", 0)
                    if count > 0:
                        output.append(
                            {
                                "DATA_PROVIDER": provider,
                                "LEGAL_TOOL": simplified_rights,
                                "THEME": theme,
                                "COUNT": count,
                            }
                        )

                except requests.RequestException as e:
                    LOGGER.warning(
                        f"Failed for provider={provider}, "
                        f"rights={rights}, "
                        f"theme={theme}: "
                        f"{e}"
                    )
                time.sleep(0.5)
    LOGGER.info(f"Aggregated {len(output)} records (with themes).")
    return output


def write_data(args, data_no_theme, data_with_theme):
    """Write Europeana data to CSV files."""
    if not args.enable_save:
        return args

    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if data_no_theme:
        with open(FILE_WITHOUT_THEMES, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=HEADER_WITHOUT_THEMES, dialect="unix"
            )
            writer.writeheader()
            writer.writerows(data_no_theme)
        LOGGER.info(
            f"Saved {len(data_no_theme)} rows to {FILE_WITHOUT_THEMES}."
        )

    if data_with_theme:
        with open(FILE_WITH_THEMES, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=HEADER_WITH_THEMES, dialect="unix"
            )
            writer.writeheader()
            writer.writerows(data_with_theme)
        LOGGER.info(
            f"Saved {len(data_with_theme)} rows to {FILE_WITH_THEMES}."
        )

    return args


def main():
    args = parse_arguments()
    LOGGER.info("Beginning fetch from Europeana")
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if not EUROPEANA_API_KEY:
        raise shared.QuantifyingException(
            "EUROPEANA_API_KEY not found in environment variables", 1
        )

    session = get_requests_session()
    data_no_theme = fetch_europeana_data_without_themes(
        session, limit=args.limit
    )
    data_with_theme = fetch_europeana_data_with_themes(
        session, THEMES, limit=args.limit
    )

    args = write_data(args, data_no_theme, data_with_theme)

    if args.enable_git:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add Europeana files (with and without themes) for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])

    LOGGER.info("Europeana fetch completed successfully.")


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
