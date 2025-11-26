# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback

# Third-party
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# First-party/Local
import shared

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

LOGGER, PATHS = shared.setup(__file__)

# Gitlab constants
FILE1_COUNT = os.path.join(PATHS["data_phase"], "gitlab_1_count.csv")
GL_TOKEN = os.getenv("GL_TOKEN")

GITLAB_TOOLS = [
    {"TOOL_IDENTIFIER": "BSD Zero Clause License", "SPDX_IDENTIFIER": "0BSD"},
    {"TOOL_IDENTIFIER": "CC0 1.0", "SPDX_IDENTIFIER": "CC0-1.0"},
    {"TOOL_IDENTIFIER": "CC BY 4.0", "SPDX_IDENTIFIER": "CC-by-4.0"},
    {"TOOL_IDENTIFIER": "CC BY-SA 4.0", "SPDX_IDENTIFIER": "CC-by-SA-4.0"},
    {"TOOL_IDENTIFIER": "MIT No Attribution", "SPDX_IDENTIFIER": "MIT-0"},
    {"TOOL_IDENTIFIER": "Unlicense", "SPDX_IDENTIFIER": "Unlicense"},
    {"TOOL_IDENTIFIER": "Total public repositories", "SPDX_IDENTIFIER": "N/A"},
]
HEADER1_COUNT = ["TOOL_IDENTIFIER", "SPDX_IDENTIFIER", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

GITLAB_API_BASE = "https://gitlab.com/api/v4"


def parse_arguments():
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
    try:
        with open(FILE1_COUNT, "r", newline="") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            if len(list(reader)) == len(GITLAB_TOOLS):
                raise shared.QuantityingException(
                    f"Data fetch completed for {QUARTER}", 0
                )
    except FileNotFoundError:
        pass


def write_data(args, tool_data):
    if not args.enable_save:
        return args
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    if len(tool_data) < len(GITLAB_TOOLS):
        LOGGER.error("Unable to fetch all records. Aborting.")
        return args
    with open(FILE1_COUNT, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER1_COUNT, dialect="unix"
        )
        writer.writeheader()
        for row in tool_data:
            writer.writerow(row)
    return args


def get_all_public_projects(session):
    """Fetch ALL public GitLab projects with pagination."""
    page = 1
    per_page = 100
    all_projects = []

    MAX_PAGES = 10
    while page <= MAX_PAGES:
        url = (
            f"{GITLAB_API_BASE}/projects"
            f"?visibility=public&simple=true"
            f"&per_page={per_page}&page={page}"
        )
        LOGGER.info(f"Fetching page {page} ...")
        response = session.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        all_projects.extend(data)
        if len(data) < per_page:
            break
        page += 1
    return all_projects


def query_gitlab(args, session):
    all_projects = get_all_public_projects(session)
    tool_data = []
    for tool in GITLAB_TOOLS:
        tool_identifier = tool["TOOL_IDENTIFIER"]
        spdx_identifier = tool["SPDX_IDENTIFIER"]
        LOGGER.info(f"Query: tool: {tool_identifier}, spdx:{spdx_identifier}")

        if tool_identifier == "Total public repositories":
            count = len(all_projects)
        else:
            count = sum(
                1
                for p in all_projects
                if p.get("license")
                and p["license"].get("spdx_id", "").lower()
                == spdx_identifier.lower()
            )
        tool_data.append(
            {
                "TOOL_IDENTIFIER": tool_identifier,
                "SPDX_IDENTIFIER": spdx_identifier,
                "COUNT": count,
            }
        )
        LOGGER.info(f"count:{count}")
    return tool_data


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    check_for_completion()

    session = shared.get_session(
        accept_header="application/json",
    )
    if GL_TOKEN:
        session.headers.update({"authorization": f"Bearer {GL_TOKEN}"})
    tool_data = query_gitlab(args, session)
    args = write_data(args, tool_data)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_phase"],
        f"Add and commit new GitLab data for {QUARTER}",
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
        sys.exit(130)
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
