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
from matplotlib import colormaps
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
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        global PATHS
        PATHS = shared.update_paths(LOGGER, PATHS, QUARTER, args.quarter)
    args.logger = LOGGER
    args.paths = PATHS
    return args


def number_formatter(x, pos):
    """
    Use the millions formatter for x-axis

    The two args are the value (x) and tick position (pos)
    """
    if x >= 1e9:
        return f"{x * 1e-9:,.0f}B"
    elif x >= 1e6:
        return f"{x * 1e-6:,.0f}M"
    elif x >= 1e3:
        return f"{x * 1e-3:,.0f}K"
    else:
        return f"{x:,.0f}"


def annotate_ylabels(ax, data, data_label, colors):
    i = 0
    c = 0
    ytick = ax.yaxis.get_major_ticks(numticks=1)[0]
    #    defaults: ytick.major.size         + ytick.major.pad
    indent = -1 * (ytick.get_tick_padding() + ytick.get_pad())
    for index, row in data.iterrows():
        if c > len(colors):
            c = 0

        # annotate totals
        ax.annotate(
            f"    {row[data_label]:>15,d}",
            (indent, i - 0.1),
            xycoords=("axes points", "data"),
            color=colors[c],
            fontsize="x-small",
            horizontalalignment="right",
            verticalalignment="top",
        )

        # annotate percentages
        percent = row[data_label] / data[data_label].sum() * 100
        if percent < 0.1:
            percent = "< .1%"
        else:
            percent = f"{percent:4.1f}%"
        ax.annotate(
            percent,
            (1.02, i),
            xycoords=("axes fraction", "data"),
            backgroundcolor=colors[c],
            color="white",
            fontsize="x-small",
            horizontalalignment="left",
            verticalalignment="center",
        )

        i += 1
        c += 1
    return ax


def combined_plot(
    args, data, title, name_label, data_label, bar_xscale=None, bar_ylabel=None
):
    if len(data) > 10:
        raise shared.QuantifyingException(
            "the combined_plot() function is limited to a maximum of 10 data"
            " points"
        )

    plt.rcParams.update({"font.family": "monospace", "figure.dpi": 300})

    height = 1 + len(data) * 0.5
    if height < 2.5:
        height = 2.5

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(8, height), width_ratios=(2, 1), layout="constrained"
    )
    colors = colormaps["tab10"].colors

    # 1st axes: horizontal barplot of counts
    # pad tick labels to make room for annotation
    tick_labels = []
    for index, row in data.iterrows():
        count = f"{row[data_label]:,d}"
        tick_labels.append(f"{index}\n{' ' * len(count)}")
    if bar_xscale == "log":
        log = True
    else:
        bar_xscale = "linear"
        log = False
    ax1.barh(y=tick_labels, width=data[data_label], color=colors, log=log)
    ax1.tick_params(axis="x", which="major", labelrotation=45)
    ax1.set_xlabel("Number of works")
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(number_formatter))
    if bar_ylabel is not None:
        ax1.set_ylabel(bar_ylabel)
    else:
        ax1.set_ylabel(name_label)
    ax1 = annotate_ylabels(ax1, data, data_label, colors)

    # 2nd axes: pie chart of percentages
    data.plot.pie(
        ax=ax2,
        y=data_label,
        colors=colors,
        labels=None,
        legend=False,
        radius=1.25,
    )
    ax2.set_title("Percent")
    ax2.set_ylabel(None)

    # plot
    plt.suptitle(title)
    plt.annotate(
        f"Creative Commons (CC)\nbar x scale: {bar_xscale}, data from"
        f" {args.quarter}",
        (0.95, 5),
        xycoords=("figure fraction", "figure points"),
        color="gray",
        fontsize="x-small",
        horizontalalignment="right",
    )

    if args.show_plots:
        plt.show()

    return plt


def gcs_intro(args):
    """
    Write Google Custom Search (GCS) introduction.
    """
    LOGGER.info(gcs_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "gcs_product_totals.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool product"
    data = pd.read_csv(file_path, index_col=name_label)
    total_count = f"{data['Count'].sum():,d}"
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
        f"**The results indicate there are a total of {total_count} online"
        " works in the commons--documents that are licensed or put in the"
        " public domain using a Creative Commons (CC) legal tool.**\n"
        "\n"
        "Thank you Google for providing the Programable Search Engine: Custom"
        " Search JSON API!\n",
    )


def plot_products(args):
    """
    Create plots for CC legal tool product totals and percentages
    """
    LOGGER.info(plot_products.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_product_totals.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool product"
    data = pd.read_csv(file_path, index_col=name_label)
    data = data[::-1]  # reverse order

    title = "Products totals and percentages "
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
        bar_ylabel=name_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_product_totals.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

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
        PATHS["data_2-process"],
        "gcs_status_combined_totals.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "CC legal tools status"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
        bar_ylabel="CC legal tool status",
    )

    image_path = shared.path_join(PATHS["data_phase"], "gcs_tool_status.png")
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

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
        PATHS["data_2-process"],
        "gcs_status_current_totals.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Current CC legal tools"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_current_tools.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

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
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Old CC legal tools"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_old_tools.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

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
        PATHS["data_2-process"],
        "gcs_status_retired_totals.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Retired CC legal tools"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_retired_tools.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

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


def plot_countries_highest_usage(args):
    """
    Create plots for the countries with highest usage of current tools
    """
    LOGGER.info(plot_countries_highest_usage.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_totals_by_country.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Country"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)
    total_count = f"{data['Count'].sum():,d}"
    data.sort_values(data_label, ascending=False, inplace=True)
    data = data[:10]  # limit to highest 10
    data = data[::-1]  # reverse order

    title = "Countries with highest usage of current tools"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_countries_highest_usage_current_tools.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing countries with the highest useage of the current"
        " Creative Commons (CC) legal tools.",
        "The current tools include Licenses version 4.0 (CC BY 4.0, CC BY-NC"
        " 4.0, CC BY-NC-ND 4.0, CC BY-NC-SA 4.0, CC-BY-ND 4.0, CC BY-SA 4.0),"
        " CC0 1.0, and the Public Domain Mark (PDM 1.0).\n"
        "\n"
        f"The complete data set indicates there are a total of {total_count}"
        " online works using a current CC legal tool.",
    )


def plot_languages_highest_usage(args):
    """
    Create plots for the languages with highest usage of current tools
    """
    LOGGER.info(plot_languages_highest_usage.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "gcs_totals_by_language.csv"
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Language"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)
    total_count = f"{data['Count'].sum():,d}"
    data.sort_values(data_label, ascending=False, inplace=True)
    data = data[:10]  # limit to highest 10
    data = data[::-1]  # reverse order

    title = "Languages with highest usage of current tools"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_languages_highest_usage_current_tools.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing languages with the highest useage of the current"
        " Creative Commons (CC) legal tools.",
        "The current tools include Licenses version 4.0 (CC BY 4.0, CC BY-NC"
        " 4.0, CC BY-NC-ND 4.0, CC BY-NC-SA 4.0, CC-BY-ND 4.0, CC BY-SA 4.0),"
        " CC0 1.0, and the Public Domain Mark (PDM 1.0).\n"
        "\n"
        f"The complete data set indicates there are a total of {total_count}"
        " online works using a current CC legal tool.",
    )


def plot_free_culture(args):
    """
    Create plots for the languages with highest usage of current tools
    """
    LOGGER.info(plot_free_culture.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "gcs_totals_by_free_cultural.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)

    title = "Approved for Free Cultural Works"
    plt = combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(PATHS["data_phase"], "gcs_free_culture.png")
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing Approved for Free Cultural Works legal tool usage.",
        "For more information on retired legal tools, see [Understanding Free"
        " Cultural Works - Creative"
        " Commons](https://creativecommons.org/public-domain/freeworks/).",
    )


def main():
    args = parse_arguments()
    shared.log_paths(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    gcs_intro(args)
    plot_products(args)
    plot_tool_status(args)
    plot_current_tools(args)
    plot_old_tools(args)
    plot_retired_tools(args)
    plot_countries_highest_usage(args)
    plot_languages_highest_usage(args)
    plot_free_culture(args)

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
