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
        PATHS["data_2-process"],
        "smithsonian_totals_by_records.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path)
    total_objects = data["Total_objects"].sum()
    CC0_records = data["CC0_records"].sum()
    CC0_records_with_media = data["CC0_records_with_CC0_media"].sum()
    CC0_media_percentage = f"{data['CC0_with_media_percentage'].mean():.2f}%"
    num_units = len(data)
    min_object = data["Total_objects"].min()
    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        "Overview",
        None,
        None,
        "The Smithsonian Institute data returns the overall"
        " statistics of CC0 legal tool records."
        " It serves as the main legal tool used by Smithsonian Institute."
        "\n"
        f"The results indicate a total record of {total_objects:,} objects,"
        f" with a breakdown of {CC0_records:,} objects without CC0 Media and"
        f" {CC0_records_with_media:,} objects with CC0 Media, taking a"
        f" percentage of {CC0_media_percentage} in each institute member."
        f" There are {num_units} unique units in the data"
        " representing museums, libraries, zoos and other institutions"
        f" with a minimum of {min_object} objects.",
    )


def plot_totals_by_top10_units(args):
    """
    Create plots showing totals by top 10 units
    """
    LOGGER.info(plot_totals_by_top10_units.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "smithsonian_totals_by_units.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Data_source"
    data_label = "Total_objects"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data["Total_objects"] = data["Total_objects"].astype(int)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    average_unit = data["Total_objects"].mean()
    title = "Totals by 10 Units"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
        bar_ylabel="Data Sources",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_totals_by_top10_units.png"
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
        "Plots showing totals by units. This shows the"
        " distribution of top 10 institute member across"
        " Smithsonian Institute with an average of"
        f" {average_unit:,} objects across the top 10"
        " Institute members.",
    )


def plot_totals_by_lowest10_units(args):
    """
    Create plots showing totals by lowest 10 units
    """
    LOGGER.info(plot_totals_by_lowest10_units.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "smithsonian_totals_by_units.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Data_source"
    data_label = "Total_objects"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data["Total_objects"] = data["Total_objects"].astype(int)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.head(10)
    average_unit = data["Total_objects"].mean()
    title = "Totals by lowest 10 Units"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
        bar_ylabel="Data Sources",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_totals_by_lowest10_unit.png"
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
        "Plots showing totals by units.",
        "This shows the distribution of lowest 10"
        " institute member across Smithsonian Institute"
        f" with an average of {average_unit} objects"
        " across the lowest 10 institute members.",
    )


def plot_totals_by_top10_unit_records(args):
    """
    Create plots showing breakdown of CC0 records by top 10 units
    """
    LOGGER.info(plot_totals_by_top10_unit_records.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "smithsonian_totals_by_records.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Data_source"
    data_label = "Total_objects"
    stack_labels = [
        "CC0_without_media_percentage",
        "CC0_with_media_percentage",
        "Others_percentage",
    ]
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "Breakdown of CC0 records by top 10 units"
    plt = plot.stacked_barh_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        stack_labels=stack_labels,
        ylabel="Data Sources",
    )
    image_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_by_top10_unit_records.png"
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
        "Plots showing totals by CC0 records. This is the"
        " top 10 units with a breakdown of CC0 records"
        " without media, CC0 records with media and records"
        " that are not associated with CC0.",
    )


def plot_totals_by_lowest10_unit_records(args):
    """
    Create plots showing breakdown of CC0 records by lowest 10 units
    """
    LOGGER.info(plot_totals_by_lowest10_unit_records.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "smithsonian_totals_by_records.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Data_source"
    data_label = "Total_objects"
    stack_labels = [
        "CC0_without_media_percentage",
        "CC0_with_media_percentage",
        "Others_percentage",
    ]
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.head(10)
    title = "Breakdown of CC0 records by lowest 10 units"
    plt = plot.stacked_barh_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        stack_labels=stack_labels,
        ylabel="Data Sources",
    )
    image_path = shared.path_join(
        PATHS["data_phase"], "smithsonian_by_lowest10_unit_records.png"
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
        "Plots showing totals by CC0 records. This is the"
        " lowest 10 units with a breakdown of CC0 records"
        " without media, CC0 records with media and records"
        " that are not associated with CC0.",
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
    plot_totals_by_top10_units(args)
    plot_totals_by_lowest10_units(args)
    plot_totals_by_top10_unit_records(args)
    plot_totals_by_lowest10_unit_records(args)

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
