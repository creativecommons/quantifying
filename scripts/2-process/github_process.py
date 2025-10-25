#!/usr/bin/env python
"""
Process GitHub CC License usage data for reporting.
Phase 2: Clean and summarize GitHub data.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback

# Third-party
# Third-party (for nicer exception formatting to match repo style)
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# First-party/Local
import shared  # noqa: E402

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Setup
LOGGER, PATHS = shared.setup(__file__)

# NOTE:
# We will use TOOL_IDENTIFIER
# (human-friendly names like "CC BY 4.0")
# as the canonical license identifier in the
# processed CSV, per reviewer guidance.


def parse_arguments():
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(
        description="Process GitHub data for reporting"
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results to data/2-process directory",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Commit and push processed files using git",
    )
    return parser.parse_args()


def load_github_counts():
    """
    Load the Phase 1 counts file (github_1_count.csv) and return rows
    as dicts with: TOOL_IDENTIFIER, LICENSE_IDENTIFIER, COUNT
    """
    file_path = os.path.join(PATHS["data_1-fetch"], "github_1_count.csv")
    if not os.path.exists(file_path):
        raise shared.QuantifyingException(
            f"GitHub fetch file not found: {file_path}", exit_code=1
        )

    out = []
    with open(file_path, "r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # COUNT may be missing or malformed; default to 0
            try:
                count = int(row.get("COUNT", "0"))
            except (ValueError, TypeError):
                count = 0

            # Use TOOL_IDENTIFIER as the canonical license
            # identifier for reporting
            tool_identifier = row.get("TOOL_IDENTIFIER", "").strip()

            out.append(
                {
                    "TOOL_IDENTIFIER": tool_identifier,
                    "LICENSE_IDENTIFIER": tool_identifier,
                    "COUNT": count,
                }
            )
    return out


def process_data(rows):
    """
    Phase 1 already includes a TOTAL row
    ("Total public repositories"),
    so we simply return the rows unchanged to avoid duplication.
    """
    return rows


def save_summary(summary, args):
    """
    Save summary to PATHS['data_2-process']/github_summary.csv
    when --enable-save.
    """
    if not args.enable_save:
        LOGGER.info("Skipping save step (--enable-save not passed)")
        return

    out_dir = PATHS["data_2-process"]
    os.makedirs(out_dir, exist_ok=True)
    output_file = os.path.join(out_dir, "github_summary.csv")

    fieldnames = ["TOOL_IDENTIFIER", "LICENSE_IDENTIFIER", "COUNT"]
    with open(output_file, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, dialect="unix")
        writer.writeheader()
        for row in summary:
            writer.writerow(row)

    LOGGER.info(f"Processed GitHub data saved to {output_file}")

    # Git behaviour
    # (no-op unless args.enable_git and user has configured remotes)
    if args.enable_git:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            "Add processed GitHub summary file",
        )
        shared.git_push_changes(args, PATHS["repo"])


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)

    rows = load_github_counts()
    summary = process_data(rows)
    save_summary(summary, args)


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
