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
    LOGGER.info(gcs_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_product_totals.csv"
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


def millions_formatter(x, pos):
    """
    Use the millions formatter for x-axis

    The two args are the value (x) and tick position (pos)
    """
    return f"{x * 1e-6:,.0f}M"


def annotate_count(ax, data, x, colors):
    # annotate totals
    i = 0
    for index, row in data.iterrows():
        ax.annotate(
            f"{row['Count']:>15,d}",
            (80, i),
            xycoords=("axes points", "data"),
            color="black",
            fontsize="small",
            fontfamily="monospace",
            horizontalalignment="right",
            verticalalignment="center",
        )
        i += 1
    # annotate percentages
    i = 0
    c = 0
    for index, row in data.iterrows():
        if c > len(colors):
            c = 0
        percent = row[x] / data[x].sum() * 100
        ax.annotate(
            f"{percent:5.2f}%",
            (1.02, i),
            xycoords=("axes fraction", "data"),
            backgroundcolor=colors[c],
            color="black",
            fontsize="x-small",
            fontfamily="monospace",
            horizontalalignment="left",
            verticalalignment="center",
        )
        i += 1
        c += 1
    return ax


def combined_plot(args, data, title, x, y, bar_xscale=None, bar_ylabel=None):

    height = 1 + len(data) * 0.25
    if height < 2.5:
        height = 2.5
    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(8, height), width_ratios=(4, 1)
    )

    colors = sns.color_palette("pastel")

    # 1st axes: horizontal barplot of counts
    data.plot.barh(ax=ax1, y=x, width=0.8, color=colors, legend=False)
    ax1 = annotate_count(ax1, data, x, colors)
    if bar_xscale is None:
        bar_xscale = "linear"
    ax1.set_xscale(bar_xscale)
    if bar_ylabel is None:
        ax1.set_ylabel("CC legal tool unit")
    else:
        ax1.set_ylabel(bar_ylabel)
    ax1.set_xlabel("Number of works")
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

    # 2nd axes: pie chart of percentages
    data.plot.pie(
        ax=ax2,
        y=x,
        colors=colors,
        labels=None,
        legend=False,
        radius=2,
    )
    ax2.set_aspect("equal")
    ax2.set_ylabel("Percent", labelpad=30)

    # plot
    plt.suptitle(title)
    plt.annotate(
        f"Creative Commons (CC)\nbar x scale: {bar_xscale}, plot generated"
        f" {QUARTER}",
        (0.95, 5),
        xycoords=("figure fraction", "figure points"),
        color="gray",
        fontsize="x-small",
        horizontalalignment="right",
    )
    # For default values, see:
    # https://matplotlib.org/stable/users/explain/customizing.html#matplotlibrc-sample
    plt.subplots_adjust(right=0.95, wspace=0.25)
    # plt.subplots_adjust(right=0.95, bottom=0.25, wspace=0.25)
    plt.tight_layout()

    if args.show_plots:
        plt.show()

    return plt


def plot_products(args):
    """
    Create plots for CC legal tool product totals and percentages
    """
    LOGGER.info(plot_products.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_product_totals.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    y = "CC legal tool product"
    data = pd.read_csv(file_path, index_col=y)

    title = "Products totals and percentages "
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        x="Count",
        y=y,
        bar_xscale="log",
        bar_ylabel="CC legal tool products",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_product_totals.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path, dpi=300)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing Creative Commons (CC) legal tool product totals and"
        " percentages.",
    )


def plot_tool_status(args):
    """
    Create plots for the CC legal tool status totals and percentages
    """
    LOGGER.info(plot_tool_status.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_status_combined_totals.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    y = "CC legal tool"
    data = pd.read_csv(file_path, index_col=y)
    data.sort_values(y, ascending=False, inplace=True)

    title = "CC legal tools status"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        x="Count",
        y=y,
        bar_xscale="log",
        bar_ylabel="CC legal tool status",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_tool_percentage.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path, dpi=300)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing Creative Commons (CC) legal tool status totals and"
        " percentages.",
    )


def plot_current_tools(args):
    """
    Create plots for current CC legal tool totals and percentages
    """
    LOGGER.info(plot_current_tools.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_status_current_totals.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    y = "CC legal tool"
    data = pd.read_csv(file_path, index_col=y)
    data.sort_values(y, ascending=False, inplace=True)

    title = "Current CC legal tool totals"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        x="Count",
        y=y,
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_current_totals.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path, dpi=300)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing current Creative Commons (CC) legal tool totals and"
        " percentages.",
    )


def plot_old_tools(args):
    """
    Create plots for old CC legal tool totals and percentages
    """
    LOGGER.info(plot_old_tools.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_status_old_totals.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    y = "CC legal tool"
    data = pd.read_csv(file_path, index_col=y)
    data.sort_values(y, ascending=False, inplace=True)

    title = "Old CC legal tool totals"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        x="Count",
        y="CC legal tool",
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_old_totals.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path, dpi=300)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing old Creative Commons (CC) legal tool totals and"
        " percentages.",
        "The unit names have been normalized (~~`CC BY-ND-NC`~~ =>"
        " `CC BY-NC-ND`).",
    )


def plot_retired_tools(args):
    """
    Create plots for retired CC legal tool totals and percentages
    """
    LOGGER.info(plot_retired_tools.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_status_retired_totals.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    y = "CC legal tool"
    data = pd.read_csv(file_path, index_col=y)
    data.sort_values(y, ascending=False, inplace=True)

    title = "Retired CC legal tools"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        x="Count",
        y="CC legal tool",
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_retired_totals.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path, dpi=300)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing retired Creative Commons (CC) legal tools total and"
        " percentages.",
        "For more information on retired legal tools, see [Retired Legal Tools"
        " - Creative Commons](https://creativecommons.org/retiredlicenses/).",
    )


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
#    countries = columns[start_indeax.margins(x=0)x:end_index]
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
#    plt.savefig(image_path, dpi=300)
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
#    plt.savefig(image_path, dpi=300)
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
    # plot_products(args)
    # plot_tool_status(args)
    plot_current_tools(args)
    # plot_old_tools(args)
    # plot_retired_tools(args)
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
