#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from Arxiv.
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
SECTION_TITLE = "Arxiv"


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


def arxiv_intro(args):
    """
    Write Arxiv introduction.
    """
    LOGGER.info(arxiv_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_1-fetch"],
        "arxiv_1_count.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    # name_label = "TOOL_IDENTIFIER"
    # data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        "Overview",
        None,
        None,
        "Coming soon",
    )


def plot_totals_by_license_type(args):
    """
    Create plots showing totals by license type
    """
    LOGGER.info(plot_totals_by_license_type.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_license.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "License"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    title = "Totals by license type"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_license_type.png"
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
        "Plots showing totals by license type.",
    )


def plot_cc_by_3_by_year(args):
    """
    Create line plot showing CC BY 3.0 by year
    """
    LOGGER.info(plot_cc_by_3_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_3_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC BY 3.0 by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_3_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC BY 3.0 works by year.",
    )


def plot_cc_by_4_by_year(args):
    """
    Create line plot showing CC BY 4.0 by year
    """
    LOGGER.info(plot_cc_by_4_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_4_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC BY 4.0 by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_4_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC BY 4.0 works by year.",
    )


def plot_cc_by_nc_nd_4_by_year(args):
    """
    Create line plot showing CC BY-NC-ND 4.0 by year
    """
    LOGGER.info(plot_cc_by_nc_nd_4_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_nc_nd_4_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC BY-NC-ND 4.0 by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_nc_nd_4_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC BY-NC-ND 4.0 works by year.",
    )


def plot_cc_by_nc_sa_3_by_year(args):
    """
    Create line plot showing CC BY-NC-SA 3.0 by year
    """
    LOGGER.info(plot_cc_by_nc_sa_3_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_nc_sa_3_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC BY-NC-SA 3.0 by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_nc_sa_3_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC BY-NC-SA 3.0 works by year.",
    )


def plot_cc_by_nc_sa_4_by_year(args):
    """
    Create line plot showing CC BY-NC-SA 4.0 by year
    """
    LOGGER.info(plot_cc_by_nc_sa_4_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_nc_sa_4_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC BY-NC-SA 4.0 by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_nc_sa_4_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC BY-NC-SA 4.0 works by year.",
    )


def plot_cc_by_sa_4_by_year(args):
    """
    Create line plot showing CC BY-SA 4.0 by year
    """
    LOGGER.info(plot_cc_by_sa_4_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_sa_4_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC BY-SA 4.0 by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_sa_4_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC BY-SA 4.0 works by year.",
    )


def plot_cc0_1_by_year(args):
    """
    Create line plot showing CC0 1.0 by year
    """
    LOGGER.info(plot_cc0_1_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc0_1_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="Year")
    title = "CC0 1.0 legal tool by year"
    plt = plot.line_plot(
        args=args,
        data=data,
        title=title,
        xlabel="Year",
        ylabel="Number of works",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc0_1_by_year.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Line plot showing CC0 1.0 works by year.",
    )


def plot_totals_by_author_bucket(args):
    """
    Create stacked vertical bar plot showing totals by author bucket
    """
    LOGGER.info(plot_totals_by_author_bucket.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_author_bucket.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = shared.open_data_file(LOGGER, file_path, index_col="AUTHOR_BUCKET")
    stack_labels = list(data.columns)
    title = "Totals by author bucket"
    plt = plot.stacked_barv_plot(
        args=args,
        data=data,
        title=title,
        name_label="Author Bucket",
        stack_labels=stack_labels,
        xlabel="Author Bucket",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_author_bucket.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Stacked bar plot showing Arxiv works by author bucket,"
        " broken down by license type.",
    )


def plot_cc_by_3_by_category(args):
    """
    Create plots showing CC BY 3.0 by category
    """
    LOGGER.info(plot_cc_by_3_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_3_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC BY 3.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_3_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC BY 3.0 totals by category.",
    )


def plot_cc_by_4_by_category(args):
    """
    Create plots showing CC BY 4.0 by category
    """
    LOGGER.info(plot_cc_by_4_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_4_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC BY 4.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_4_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC BY 4.0 totals by category.",
    )


def plot_cc_by_nc_nd_4_by_category(args):
    """
    Create plots showing CC BY-NC-ND 4.0 by category
    """
    LOGGER.info(plot_cc_by_nc_nd_4_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_nc_nd_4_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC BY-NC-ND 4.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_nc_nd_4_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC BY-NC-ND 4.0 totals by category.",
    )


def plot_cc_by_nc_sa_3_by_category(args):
    """
    Create plots showing CC BY-NC-SA 3.0 by category
    """
    LOGGER.info(plot_cc_by_nc_sa_3_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_nc_sa_3_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC BY-NC-SA 3.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_nc_sa_3_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC BY-NC-SA 3.0 totals by category.",
    )


def plot_cc_by_nc_sa_4_by_category(args):
    """
    Create plots showing CC BY-NC-SA 4.0 by category
    """
    LOGGER.info(plot_cc_by_nc_sa_4_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_nc_sa_4_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC BY-NC-SA 4.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_nc_sa_4_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC BY-NC-SA 4.0 totals by category.",
    )


def plot_cc_by_sa_4_by_category(args):
    """
    Create plots showing CC BY-SA 4.0 by category
    """
    LOGGER.info(plot_cc_by_sa_4_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc_by_sa_4_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC BY-SA 4.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc_by_sa_4_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC BY-SA 4.0 totals by category.",
    )


def plot_cc0_1_by_category(args):
    """
    Create plots showing CC0 1.0 by category
    """
    LOGGER.info(plot_cc0_1_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_cc0_1_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    data = data.tail(10)
    title = "CC0 1.0 by category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "arxiv_cc0_1_by_category.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION_FILE,
        SECTION_TITLE,
        title,
        image_path,
        "Plots showing CC0 1.0 totals by category.",
    )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    last_entry = shared.path_join(
        PATHS["data_phase"], "github_restriction.png"
    )
    shared.check_completion_file_exists(args, last_entry)
    arxiv_intro(args)
    plot_totals_by_license_type(args)
    plot_cc_by_3_by_year(args)
    plot_cc_by_4_by_year(args)
    plot_cc_by_nc_nd_4_by_year(args)
    plot_cc_by_nc_sa_3_by_year(args)
    plot_cc_by_nc_sa_4_by_year(args)
    plot_cc_by_sa_4_by_year(args)
    plot_cc0_1_by_year(args)
    plot_totals_by_author_bucket(args)
    plot_cc_by_3_by_category(args)
    plot_cc_by_4_by_category(args)
    plot_cc_by_nc_nd_4_by_category(args)
    plot_cc_by_nc_sa_3_by_category(args)
    plot_cc_by_nc_sa_4_by_category(args)
    plot_cc_by_sa_4_by_category(args)
    plot_cc0_1_by_category(args)

    # Add and commit changes
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit GitHub reports for {QUARTER}",
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
