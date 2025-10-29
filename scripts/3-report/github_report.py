#!/usr/bin/env python
"""
GitHub Reporting Script - Phase 3
Generates visual report of GitHub CC license usage based on processed CSV.
"""

# Standard library
import argparse
import os
import sys
import textwrap
import traceback

# Third-party
import matplotlib.pyplot as plt
import pandas as pd

# Third-party for nicer tracebacks (repo style)
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Creative Commons Brand Colors (fallback gray for unknown)
CC_COLORS = {
    "CC0 1.0": "#A7A9AC",  # CC0 Grey
    "CC BY 4.0": "#1477D4",  # CC Blue
    "CC BY-SA 4.0": "#00A35C",  # CC Green
    "MIT No Attribution": "#FFDD00",  # CC Yellow
    "Unlicense": "#000000",  # CC Black
    "BSD Zero Clause License": "#FF100F",  # CC Red
    "CC BY-NC 4.0": "#E14D94",  # CC Pink
    "CC BY-NC-SA 4.0": "#8C4799",  # CC Purple
    # Other licenses will be default gray
}


def parse_arguments():
    parser = argparse.ArgumentParser(description="GitHub License Report")
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving the report to data/<quarter>/3-report",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Commit and push generated reports",
    )
    parser.add_argument(
        "--show-plots",
        action="store_true",
        help="Display charts while generating them",
    )
    return parser.parse_args()


def load_processed_data():
    """
    Load the Phase 2 output (github_summary.csv).
    """
    file_path = os.path.join(PATHS["data_2-process"], "github_summary.csv")
    if not os.path.exists(file_path):
        raise shared.QuantifyingException(
            f"Processed GitHub summary file not found: {file_path}",
            exit_code=1,
        )

    df = pd.read_csv(file_path)

    # Removing total repositories row if present
    if (
        "total public repositories"
        in df["LICENSE_IDENTIFIER"].str.lower().values
    ):
        # remove "total public repositories" row
        df = df[
            df["LICENSE_IDENTIFIER"].str.lower() != "total public repositories"
        ]

    # Clean column names
    df.columns = [c.strip() for c in df.columns]

    # Strip whitespace from LICENSE_IDENTIFIER
    df["LICENSE_IDENTIFIER"] = df["LICENSE_IDENTIFIER"].astype(str).str.strip()

    # Capture TOTAL row separately
    total_row = df[df["LICENSE_IDENTIFIER"].str.upper() == "TOTAL"]
    total_count = (
        int(total_row["COUNT"].values[0]) if not total_row.empty else 0
    )

    # Filter out TOTAL from plot data
    df_plot = df[df["LICENSE_IDENTIFIER"].str.upper() != "TOTAL"].copy()
    df_plot["COUNT"] = (
        pd.to_numeric(df_plot["COUNT"], errors="coerce").fillna(0).astype(int)
    )
    df_plot = df_plot.sort_values(by="COUNT", ascending=False)

    return df_plot, total_count


def create_license_bar_chart(df, args):
    """
    Generate bar chart from license data.
    """
    plt.figure(figsize=(12, 8))

    labels = df["LICENSE_IDENTIFIER"].tolist()
    counts = df["COUNT"].tolist()
    colors = [CC_COLORS.get(lic, "#808080") for lic in labels]

    bars = plt.bar(labels, counts, color=colors)
    plt.title("GitHub Repositories by License (Creative Commons)")
    plt.xlabel("License")
    plt.ylabel("Repository Count")
    plt.xticks(rotation=45, ha="right")

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        plt.annotate(
            format(height, ",d"),
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 6),
            textcoords="offset points",
            ha="center",
            va="bottom",
        )

    plt.tight_layout()

    out_dir = PATHS["data_3-report"]
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "github_license_report.png")
    plt.savefig(out_file)

    if args.show_plots:
        plt.show()

    LOGGER.info(f"Saved license report to: {out_file}")
    return out_file


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)

    df, total_count = load_processed_data()

    # Log total repositories
    LOGGER.info(f"Total GitHub repositories analyzed: {total_count:,}")

    create_license_bar_chart(df, args)

    # Optionally commit/push
    if args.enable_git:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            "Add GitHub license report",
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
