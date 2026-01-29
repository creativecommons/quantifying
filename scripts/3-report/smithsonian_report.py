#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from Smithsonian.
"""
# Standard library
import argparse
import os
import sys
import textwrap
import traceback
from pathlib import Path

# Third-party
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import plot  # noqa: E402
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)
QUARTER = os.path.basename(PATHS["data_quarter"])
SECTION_FILE = Path(__file__).name
SECTION_TITLE = "Smithsonian"


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    global QUARTER
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=QUARTER,
        help=f"Data quarter in format YYYYQx (default: {QUARTER})",
    )
    parser.add_argument(
        "--show-plots",
        action="store_true",
        help="Show generated plots (default: False)",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results (default: False)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions such as fetch, merge, add, commit, and push"
        " (default: False)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate data even if report files exist",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        global PATHS
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
        QUARTER = args.quarter
    args.logger = LOGGER
    args.paths = PATHS
    return args


def load_data(args):
    """
    Load the collected data from the CSV file.
    """
    selected_quarter = args.quarter

    file_path = os.path.join(
        PATHS["data"],
        f"{selected_quarter}",
        "1-fetch",
        "smithsonian_1_metrics.csv",
    )

    data = shared.open_data_file(LOGGER, file_path)

    LOGGER.info(f"Data loaded from {file_path}")
    return data


def smithsonian_intro(args):
    """
    Write Smithsonian introduction.
    """
    LOGGER.info(smithsonian_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_1-fetch"],
        "smithsonian_1_metrics.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    # name_label = "UNIT"
    # data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    # data.sort_values(name_label, ascending=True, inplace=True)
    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        "Overview",
        None,
        None,
    )


def plot_totals_by_units(args):
    """
    Create plots showing totals by units
    """
    LOGGER.info(plot_totals_by_units.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "smithsonian_totals_by_units.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Unit"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.head(10)
    title = "Totals by Units"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_totals_by_unit.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Coming soon",
    )


def plot_totals_by_records(args):
    """
    Create plots showing totals by records
    """
    LOGGER.info(plot_totals_by_records.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "smithsonian_totals_by_records.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Unit"
    stack_labels = [
        "CC0_RECORDS_PERCENTAGE",
        "CC0_RECORDS_WITH_CC0_MEDIA_PERCENTAGE",
    ]
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(stack_labels, ascending=False, inplace=True)
    data = data.head(10)
    title = "Totals by records"
    plt = plot.stacked_barh_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        stack_labels=stack_labels,
    )
    image_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_by_records.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Coming soon",
    )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    last_entry = shared.path_join(
        PATHS["data_phase"], "smithsonian_by_records.png"
    )
    shared.check_completion_file_exists(args, last_entry)
    smithsonian_intro(args)
    plot_totals_by_units(args)
    plot_totals_by_records(args)

    # Add and commit changes
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit Smithsonian reports for {QUARTER}",
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
