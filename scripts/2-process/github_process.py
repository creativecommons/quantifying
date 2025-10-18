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
import traceback

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)


def parse_arguments():
    """
    Parse command-line options.
    """
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
    Load GitHub license counts from Phase 1.
    """
    file_path = os.path.join(PATHS["data_1-fetch"], "github_1_count.csv")
    if not os.path.exists(file_path):
        raise shared.QuantifyingException(
            f"GitHub fetch file not found: {file_path}", exit_code=1
        )

    data = []
    with open(file_path, "r", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                count = int(row["COUNT"])
            except ValueError:
                count = 0
            data.append(
                {
                    "TOOL_IDENTIFIER": row["TOOL_IDENTIFIER"],
                    "SPDX_IDENTIFIER": row["SPDX_IDENTIFIER"],
                    "COUNT": count,
                }
            )
    return data


def process_data(data):
    """
    Compute totals for GitHub usage.
    """
    total_count = sum(row["COUNT"] for row in data)
    summary = data.copy()
    summary.append(
        {
            "TOOL_IDENTIFIER": "TOTAL",
            "SPDX_IDENTIFIER": "N/A",
            "COUNT": total_count,
        }
    )
    return summary


def save_summary(summary, args):
    """
    Save the processed summary file.
    """
    if not args.enable_save:
        LOGGER.info("Skipping save step (--enable-save not passed)")
        return

    output_file = os.path.join(PATHS["data_2-process"], "github_summary.csv")
    os.makedirs(PATHS["data_2-process"], exist_ok=True)

    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(
            file, fieldnames=["TOOL_IDENTIFIER", "SPDX_IDENTIFIER", "COUNT"]
        )
        writer.writeheader()
        writer.writerows(summary)

    LOGGER.info(f"Processed GitHub data saved to {output_file}")

    if args.enable_git:
        shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            "Add processed GitHub summary file",
        )
        shared.git_push_changes(args, PATHS["repo"])


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)

    github_data = load_github_counts()
    summary = process_data(github_data)
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
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
