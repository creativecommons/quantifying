#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from Google Custom Search (GCS).
"""
# Standard library
import argparse
import os
import sys
import textwrap
import traceback

# Third-party
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
SECTION = "Google Custom Search (GCS)"


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=QUARTER,
        help="Data quarter in format YYYYQx, e.g., 2024Q2",
    )
    parser.add_argument(
        "--show-plots",
        action="store_true",
        help="Show generated plots (in addition to saving them)",
    )
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
    args.logger = LOGGER
    args.paths = PATHS
    return args


def gcs_intro(args):
    """
    Write Google Custom Search (GCS) introduction.
    """
    LOGGER.info(plot_totals_by_product.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_totals_by_product.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)
    shared.update_readme(
        args,
        SECTION,
        "Overview",
        None,
        None,
        "Google Custom Search (GCS) data uses the `totalResults` returned by"
        " API for search queries of the legal tool URLs (quoted and using"
        " `linkSite` for accuracy), countries codes, and language codes.\n"
        "\n"
        f"**The results show there are a total of {data['Count'].sum():,d}"
        " online documents in the commons--documents that are licensed or put"
        " in the public domain using a Creative Commons (CC) legal tool.**\n"
        "\n"
        "Thank you Google for providing the Programable Search Engine: Custom"
        " Search JSON API!\n",
    )


def plot_top_25_tools(args):
    """
    Create a bar chart for the top 25 legal tools
    """
    LOGGER.info(plot_totals_by_product.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_top_25_tools.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)

    plt.figure(figsize=(10, 10))
    y_column = "CC legal tool"
    ax = sns.barplot(
        data,
        x="Count",
        y=y_column,
        hue=y_column,
        palette="pastel",
        legend=False,
    )
    for index, row in data.iterrows():
        ax.annotate(
            f"{row['Count']:,d}",
            (4 + 80, index),
            xycoords=("axes points", "data"),
            color="black",
            fontsize="small",
            horizontalalignment="right",
            verticalalignment="center",
        )
    plt.title(f"Top 25 legal tools ({args.quarter})")
    plt.xlabel("Number of works")
    plt.ylabel("Creative Commons (CC) legal tool")

    # Use the millions formatter for x-axis
    def millions_formatter(x, pos):
        """
        The two args are the value and tick position
        """
        return f"{x * 1e-6:.0f}M"

    ax.xaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

    plt.tight_layout()
    if args.show_plots:
        plt.show()

    image_path = shared.path_join(PATHS["data_phase"], "gcs_top_25_tools.png")
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION,
        "Top 25 legal tools",
        image_path,
        "Bar chart showing the top 25 individual legal tools.",
    )

    LOGGER.info("Visualization by license type created.")


def plot_totals_by_product(args):
    """
    Create a bar chart of the totals by product
    """
    LOGGER.info(plot_totals_by_product.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_totals_by_product.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    data = pd.read_csv(file_path)

    plt.figure(figsize=(10, 5))
    y_column = "CC legal tool product"
    ax = sns.barplot(
        data,
        x="Count",
        y=y_column,
        hue=y_column,
        palette="pastel",
        legend=False,
    )
    for index, row in data.iterrows():
        ax.annotate(
            f"{row['Count']:>15,d}",
            (0 + 80, index),
            xycoords=("axes points", "data"),
            color="black",
            fontsize="small",
            horizontalalignment="right",
            verticalalignment="center",
        )
    plt.title(f"Totals by product ({args.quarter})")
    plt.ylabel("Creative Commons (CC) legal tool product")
    plt.xscale("log")
    plt.xlabel("Number of works")

    # Use the millions formatter for x-axis
    def millions_formatter(x, pos):
        """
        The two args are the value and tick position
        """
        return f"{x * 1e-6:,.0f}M"

    ax.xaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

    plt.tight_layout()
    if args.show_plots:
        plt.show()

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_totals_by_product.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION,
        "Totals by product",
        image_path,
        "Bar chart showing how many documents there are for each Creative"
        " Commons (CC) legal tool product.",
    )

    LOGGER.info("Visualization by license type created.")


# def plot_by_country(data, args):
#    """
#    Create a bar chart for the number of webpages licensed by country.
#    """
#    LOGGER.info(
#        "Creating a bar chart for the number of webpages licensed by country."
#    )
#
#    selected_quarter = args.quarter
#
#    # Get the list of country columns dynamically
#    columns = [col.strip() for col in data.columns.tolist()]
#
#    start_index = columns.index("United States")
#    end_index = columns.index("Japan") + 1
#
#    countries = columns[start_index:end_index]
#
#    data.columns = data.columns.str.strip()
#
#    LOGGER.info(f"Cleaned Columns: {data.columns.tolist()}")
#
#    # Aggregate the data by summing the counts for each country
#    country_data = data[countries].sum()
#
#    plt.figure(figsize=(12, 8))
#    ax = sns.barplot(x=country_data.index, y=country_data.values)
#    plt.title(
#        f"Number of Google Webpages Licensed by Country ({selected_quarter})"
#    )
#    plt.xlabel("Country")
#    plt.ylabel("Number of Webpages")
#    plt.xticks(rotation=45)
#
#    # Add value numbers to the top of each bar
#    for p in ax.patcplot_totals_by_producthes:
#        ax.annotate(
#            format(p.get_height(), ",.0f"),
#            (p.get_x() + p.get_width() / 2.0, p.get_height()),
#            ha="center",
#            va="center",
#            xytext=(0, 9),
#            textcoords="offset points",
#        )
#
#    # Format the y-axis to display numbers without scientific notation
#    ax.get_yaxis().get_major_formatter().set_scientific(False)
#    ax.get_yaxis().set_major_formatter(
#        plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x)))
#    )
#
#    output_directory = os.path.join(
#        PATHS["data"], f"{selected_quarter}", "3-report"
#    )
#
#    LOGGER.info(f"Output directory: {output_directory}")
#
#    # Create the directory if it does not exist
#    os.makedirs(output_directory, exist_ok=True)
#    image_path = os.path.join(output_directory, "gcs_country_report.png")
#    plt.savefig(image_path)
#
#    if args.show_plots:
#        plt.show()
#
#    shared.update_readme(
#        args,
#        SECTION,
#        "Country Report",
#        image_path,
#        "Number of Google Webpages Licensed by Country",
#    )
#
#    LOGGER.info("Visualization by country created.")
#
#
# def plot_by_language(data, args)data/2024Q4/README.md:
#    """
#    Create a bar chart for the number of webpages licensed by language.
#    """
#    LOGGER.info(
#        "Creating a bar chart for the number of webpages licensed by"
#        " language."
#    )
#
#    selected_quarter = args.quarter
#
#    # Get the list of country columns dynamically
#    columns = [col.strip() for col in data.columns.tolist()]
#
#    start_index = columns.index("English")
#    end_index = columns.index("Indonesian") + 1
#
#    languages = columns[start_index:end_index]
#
#    data.columns = data.columns.str.strip()
#
#    LOGGER.info(f"Cleaned Columns: {data.columns.tolist()}")
#
#    # Aggregate the data by summing the counts for each country
#    language_data = data[languages].sum()
#
#    plt.figure(figsize=(12, 8))
#    ax = sns.barplot(x=language_data.index, y=language_data.values)
#    plt.title(
#        f"Number of Google Webpages Licensed by Language ({selected_quarter})"
#    )
#    plt.xlabel("Language")
#    plt.ylabel("Number of Webpages")
#    plt.xticks(rotation=45)
#
#    # Add value numbers to the top of each bar
#    for p in ax.patches:
#        ax.annotate(
#            format(p.get_height(), ",.0f"),
#            (p.get_x() + p.get_width() / 2.0, p.get_height()),
#            ha="center",
#            va="center",
#            xytext=(0, 9),
#            textcoords="offset points",
#        )
#
#    # Format the y-axis to display numbers without scientific notation
#    ax.get_yaxis().get_major_formatter().set_scientific(False)
#    ax.get_yaxis().set_major_formatter(
#        plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x)))
#    )
#
#    output_directory = os.path.join(
#        PATHS["data"], f"{selected_quarter}", "3-report"
#    )
#
#    LOGGER.info(f"Output directory: {output_directory}")
#
#    # Create the directory if it does not exist
#    os.makedirs(output_directory, exist_ok=True)
#    image_path = os.path.join(output_directory, "gcs_language_report.png")
#    plt.savefig(image_path)
#
#    if args.show_plots:
#        plt.show()
#
#    shared.update_readme(
#        args,
#        SECTION,
#        "Language Report",
#        image_path,
#        "Number of Google Webpages Licensed by Language",
#    )
#
#    LOGGER.info("Visualization by language created.")


def main():
    args = parse_arguments()
    shared.log_paths(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    gcs_intro(args)
    plot_totals_by_product(args)
    plot_top_25_tools(args)
    # plot_by_country(data, args)
    # plot_by_language(data, args)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit Google Custom Search (GCS) reports for {QUARTER}",
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
