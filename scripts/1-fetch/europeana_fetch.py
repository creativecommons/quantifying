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
from operator import itemgetter

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
HEADER_WITH_THEMES = ["DATA_PROVIDER", "THEME", "LEGAL_TOOL", "COUNT"]
HEADER_WITHOUT_THEMES = ["DATA_PROVIDER", "LEGAL_TOOL", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])
TIMEOUT = 25
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
        help="Limit number of data providers",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_requests_session():
    """Create a requests session with retry."""
    max_retries = Retry(
        total=5, backoff_factor=10, status_forcelist=shared.STATUS_FORCELIST
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
    """
    Fetch complete facet list from Europeana API for a given facet field,
    returning both label and count, sorted by count descending.
    Returns: list of dicts: [{'label': ..., 'count': ...}]
    """
    all_values = []
    offset = 0
    limit = 1000

    LOGGER.info(f"Fetching {facet_field} facet values with counts.")

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
            resp = session.get(BASE_URL, params=params, timeout=TIMEOUT)
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
        for f in fields:
            label = f.get("label")
            count = f.get("count", 0)
            if label and not any(d["label"] == label for d in all_values):
                all_values.append({"label": label, "count": count})

        if len(fields) < limit:
            break

        offset += limit
        time.sleep(0.5)

    LOGGER.info(
        f"Completed fetching {facet_field}. Total unique: {len(all_values)}"
    )

    # Sort by count descending
    all_values.sort(key=itemgetter("count"), reverse=True)
    return all_values


def fetch_europeana_data_without_themes(
    session, providers_full, rights_full, limit=None
):
    """
    Fetch counts per DATA_PROVIDER × RIGHTS using pre-fetched facets.
    """
    output = []

    # Filter non-zero providers
    providers_nonzero = [p["label"] for p in providers_full if p["count"] > 0]
    if limit:
        providers_nonzero = providers_nonzero[:limit]

    # Filter non-zero rights
    rights_nonzero = [r["label"] for r in rights_full if r["count"] > 0]

    for i, provider in enumerate(providers_nonzero, start=1):
        LOGGER.info(
            f"[{i}/{len(providers_nonzero)}] "
            f"Fetching counts for provider={provider}"
        )

        for rights_url in rights_nonzero:
            simplified_rights = simplify_legal_tool(rights_url)
            query = f'DATA_PROVIDER:"{provider}" AND RIGHTS:"{rights_url}"'
            params_detail = {
                "wskey": EUROPEANA_API_KEY,
                "rows": 0,
                "query": query,
            }
            try:
                resp = session.get(
                    BASE_URL, params=params_detail, timeout=TIMEOUT
                )
                resp.raise_for_status()
                count = resp.json().get("totalResults", 0)
                if count > 0:
                    output.append(
                        {
                            "DATA_PROVIDER": provider,
                            "LEGAL_TOOL": simplified_rights,
                            "COUNT": count,
                        }
                    )
            except requests.RequestException as e:
                LOGGER.warning(
                    f"Failed for provider={provider}, rights={rights_url}: {e}"
                )
            time.sleep(0.01)

    # Sort by DATA_PROVIDER, LEGAL_TOOL
    output = sorted(output, key=itemgetter("DATA_PROVIDER", "LEGAL_TOOL"))

    LOGGER.info(
        f"Aggregated {len(output)} records for provider-rights counts."
    )
    return output


def fetch_europeana_data_with_themes(
    session, providers_full, rights_full, themes, limit=None
):
    """
    Fetch counts per DATA_PROVIDER × RIGHTS × THEME
    Uses pre-fetched providers_full and rights_full lists.
    """
    output = []

    # Filter non-zero providers
    providers_nonzero = [p["label"] for p in providers_full if p["count"] > 0]
    if limit:
        providers_nonzero = providers_nonzero[:limit]

    # Filter non-zero rights
    rights_nonzero = [r["label"] for r in rights_full if r["count"] > 0]

    for i, provider in enumerate(providers_nonzero, start=1):
        LOGGER.info(
            f"[{i}/{len(providers_nonzero)}]"
            f"Fetching rights+theme counts for provider={provider}"
        )

        for rights_url in rights_nonzero:
            simplified_rights = simplify_legal_tool(rights_url)
            for theme in themes:
                query = f'DATA_PROVIDER:"{provider}" AND RIGHTS:"{rights_url}"'
                params_detail = {
                    "wskey": EUROPEANA_API_KEY,
                    "rows": 0,
                    "query": query,
                    "theme": theme,
                }
                try:
                    resp = session.get(
                        BASE_URL, params=params_detail, timeout=TIMEOUT
                    )
                    resp.raise_for_status()
                    count = resp.json().get("totalResults", 0)
                    if count > 0:
                        output.append(
                            {
                                "DATA_PROVIDER": provider,
                                "THEME": theme,
                                "LEGAL_TOOL": simplified_rights,
                                "COUNT": count,
                            }
                        )
                except requests.RequestException as e:
                    LOGGER.warning(
                        f"Failed for provider={provider},"
                        f"rights={rights_url}, theme={theme}: {e}"
                    )
                time.sleep(0.01)

    # Sort by DATA_PROVIDER, THEME, LEGAL_TOOL
    output = sorted(
        output, key=itemgetter("DATA_PROVIDER", "THEME", "LEGAL_TOOL")
    )

    LOGGER.info(
        f"Aggregated {len(output)} records for provider-rights-theme counts."
    )
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

    # Fetch facet lists once, including counts
    providers_full = get_facet_list(session, "DATA_PROVIDER")
    rights_full = get_facet_list(session, "RIGHTS")

    # Pass facets to fetch functions
    data_no_theme = fetch_europeana_data_without_themes(
        session, providers_full, rights_full, limit=args.limit
    )
    data_with_theme = fetch_europeana_data_with_themes(
        session, providers_full, rights_full, THEMES, limit=args.limit
    )

    # Write to CSV and optionally push to git
    args = write_data(args, data_no_theme, data_with_theme)
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
