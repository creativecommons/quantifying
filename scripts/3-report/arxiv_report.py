#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from ArXiv.
"""
# Standard library
import argparse
import os
import sys
import textwrap
import traceback

# Third-party
import pandas as pd
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
SECTION = "ArXiv data"


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
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
    return parser.parse_args()


def arxiv_intro(args):
    """
    Write ArXiv introduction.
    """
    LOGGER.info(arxiv_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_license.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)

    total_papers = data["Count"].sum()
    cc_data = data[data["License"].str.startswith("CC")]
    cc_total = cc_data["Count"].sum()
    cc_percentage = f"{(cc_total / total_papers) * 100:.2f}%"

    shared.update_readme(
        args,
        SECTION,
        "Overview",
        None,
        None,
        "The ArXiv data, below, represents academic papers and preprints"
        " that use Creative Commons licenses or are in the public domain."
        "\n"
        f"**The results indicate that {cc_total} ({cc_percentage})"
        f"** of the {total_papers} papers in our dataset"
        " use identifiable CC legal tools.**\n"
        "\n"
        "ArXiv is a free distribution service and an open-access archive"
        " for scholarly articles in physics, mathematics, computer science,"
        " quantitative biology, quantitative finance, statistics,"
        " electrical engineering and systems science, and economics."
        " The data showcases the adoption of open licensing in academic"
        " research across different fields and over time."
        "\n"
        "Thank you ArXiv for providing public API"
        " access to academic paper metadata!",
    )


def plot_totals_by_license(args):
    """
    Create plots showing totals by license type
    """
    LOGGER.info(plot_totals_by_license.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_license.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)
    data.set_index("License", inplace=True)
    data.sort_values("Count", ascending=True, inplace=True)

    title = "Totals by license type"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label="License",
        data_label="Count",
    )

    if args.enable_save:
        image_path = shared.path_join(
            PATHS["data_phase"], f"arxiv_totals_by_license_{QUARTER}.png"
        )
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    if args.show_plots:
        plt.show()

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path if args.enable_save else None,
        "Plots showing ArXiv papers by license type.",
    )


def plot_totals_by_category(args):
    """
    Create plots showing totals by academic category
    """
    LOGGER.info(plot_totals_by_category.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_category.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)
    data = data.head(10)  # Top 10 for readability
    data.set_index("Category_Name", inplace=True)
    data.sort_values("Count", ascending=True, inplace=True)

    title = "Totals by academic category"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label="Category_Name",
        data_label="Count",
    )

    if args.enable_save:
        image_path = shared.path_join(
            PATHS["data_phase"], f"arxiv_totals_by_category_{QUARTER}.png"
        )
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    if args.show_plots:
        plt.show()

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path if args.enable_save else None,
        "Plots showing top ArXiv categories using CC licenses.",
    )


def plot_totals_by_year(args):
    """
    Create plots showing totals by year
    """
    LOGGER.info(plot_totals_by_year.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_year.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)
    data = data.tail(10)  # Last 10 years for readability
    data.set_index("Year", inplace=True)
    data.sort_values("Count", ascending=True, inplace=True)

    title = "Totals by year"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label="Year",
        data_label="Count",
    )

    if args.enable_save:
        image_path = shared.path_join(
            PATHS["data_phase"], f"arxiv_totals_by_year_{QUARTER}.png"
        )
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    if args.show_plots:
        plt.show()

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path if args.enable_save else None,
        "Plots showing ArXiv CC-licensed papers over time.",
    )


def plot_totals_by_author_bucket(args):
    """
    Create plots showing totals by author count bucket
    """
    LOGGER.info(plot_totals_by_author_bucket.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "arxiv_totals_by_author_bucket.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)

    if data.empty:
        LOGGER.warning("No author bucket data available for plotting")
        return

    data.set_index("Author_Bucket", inplace=True)
    data.sort_values("Count", ascending=True, inplace=True)

    title = "Totals by author count"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label="Author_Bucket",
        data_label="Count",
    )

    if args.enable_save:
        image_path = shared.path_join(
            PATHS["data_phase"], f"arxiv_totals_by_author_bucket_{QUARTER}.png"
        )
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    if args.show_plots:
        plt.show()

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path if args.enable_save else None,
        "Plots showing ArXiv papers by number of authors.",
    )


def main():
    args = parse_arguments()
    args.logger = LOGGER
    args.paths = PATHS
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    arxiv_intro(args)
    plot_totals_by_license(args)
    plot_totals_by_category(args)
    plot_totals_by_year(args)
    plot_totals_by_author_bucket(args)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new ArXiv reports for {QUARTER}",
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
