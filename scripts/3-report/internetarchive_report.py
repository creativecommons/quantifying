#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from Internet Archive (IA).
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

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
SECTION = "Internet Archive (IA)"


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
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
    args.logger = LOGGER
    args.paths = PATHS
    return args


def ia_intro(args):
    """
    Write Internet Archive (IA) introduction.
    """
    LOGGER.info(ia_intro.__doc__.strip())
    
    # Try to get total count from license totals
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "ia_license_totals.csv",
    )
    
    if os.path.exists(file_path):
        LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
        data = pd.read_csv(file_path)
        total_count = f"{data['COUNT'].sum():,d}"
    else:
        total_count = "N/A"
    
    shared.update_readme(
        args,
        SECTION,
        "Overview",
        None,
        None,
        "Internet Archive (IA) data uses the Advanced Search API to query for"
        " items with Creative Commons and open source licenses. The data includes"
        " license information, language, country, and media type metadata.\n"
        "\n"
        f"**The results indicate there are a total of {total_count} items in the"
        " Internet Archive that are licensed or in the public domain using"
        " Creative Commons or open source legal tools.**\n"
        "\n"
        "Thank you Internet Archive for providing access to this valuable"
        " cultural heritage data!\n",
    )


def plot_cc_products(args):
    """
    Create plots for CC legal tool product totals and percentages
    """
    LOGGER.info(plot_cc_products.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "ia_cc_product_totals.csv"
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool product"
    data = pd.read_csv(file_path, index_col=name_label)
    data = data[::-1]  # reverse order

    title = "CC Products totals and percentages"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
        bar_ylabel=name_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_product_totals.png"
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
        " percentages from Internet Archive.",
    )


def plot_cc_tool_status(args):
    """
    Create plots for the CC legal tool status totals and percentages
    """
    LOGGER.info(plot_cc_tool_status.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "ia_cc_status_combined_totals.csv",
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    
    # Check if data is empty
    if data.empty or data['Count'].sum() == 0:
        LOGGER.warning(f"No data found in {file_path}, skipping plot")
        return
    
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "CC legal tools status"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
        bar_ylabel="CC legal tool status",
    )

    image_path = shared.path_join(PATHS["data_phase"], "ia_cc_tool_status.png")
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
        " percentages from Internet Archive.",
    )


def plot_latest_tools(args):
    """
    Create plots for latest CC legal tool totals and percentages
    """
    LOGGER.info(plot_latest_tools.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "ia_cc_status_latest_totals.csv",
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    
    # Check if data is empty
    if data.empty or data['Count'].sum() == 0:
        LOGGER.warning(f"No data found in {file_path}, skipping plot")
        return
    
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Latest CC legal tools"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_status_latest_tools.png"
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
        "Plots showing latest Creative Commons (CC) legal tool totals and"
        " percentages from Internet Archive.",
    )


def plot_prior_tools(args):
    """
    Create plots for prior CC legal tool totals and percentages
    """
    LOGGER.info(plot_prior_tools.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "ia_cc_status_prior_totals.csv"
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    
    # Check if data is empty
    if data.empty or data['Count'].sum() == 0:
        LOGGER.warning(f"No data found in {file_path}, skipping plot")
        return
    
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Prior CC legal tools"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_status_prior_tools.png"
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
        "Plots showing prior Creative Commons (CC) legal tool totals and"
        " percentages from Internet Archive.",
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
        "ia_cc_status_retired_totals.csv",
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path, index_col=name_label)
    
    # Check if data is empty
    if data.empty or data['Count'].sum() == 0:
        LOGGER.warning(f"No data found in {file_path}, skipping plot")
        return
    
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Retired CC legal tools"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_status_retired_tools.png"
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
        " percentages from Internet Archive.",
        "For more information on retired legal tools, see [Retired Legal Tools"
        " - Creative Commons](https://creativecommons.org/retiredlicenses/).",
    )


def plot_countries_highest_usage(args):
    """
    Create plots for the countries with highest usage of latest tools
    """
    LOGGER.info(plot_countries_highest_usage.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "ia_cc_totals_by_country.csv"
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Country"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)
    total_count = f"{data['Count'].sum():,d}"
    data.sort_values(data_label, ascending=False, inplace=True)
    data = data[:10]  # limit to highest 10
    data = data[::-1]  # reverse order

    title = "Countries with highest usage of CC tools"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_countries_highest_usage.png"
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
        "Plots showing countries with the highest usage of Creative Commons"
        " (CC) legal tools from Internet Archive.",
        f"The complete data set indicates there are a total of {total_count}"
        " items using CC legal tools in the Internet Archive.",
    )


def plot_languages_highest_usage(args):
    """
    Create plots for the languages with highest usage of CC tools
    """
    LOGGER.info(plot_languages_highest_usage.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"], "ia_cc_totals_by_language.csv"
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Language"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)
    total_count = f"{data['Count'].sum():,d}"
    data.sort_values(data_label, ascending=False, inplace=True)
    data = data[:10]  # limit to highest 10
    data = data[::-1]  # reverse order

    title = "Languages with highest usage of CC tools"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_languages_highest_usage.png"
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
        "Plots showing languages with the highest usage of Creative Commons"
        " (CC) legal tools from Internet Archive.",
        f"The complete data set indicates there are a total of {total_count}"
        " items using CC legal tools in the Internet Archive.",
    )


def plot_free_culture(args):
    """
    Create plots for Approved for Free Cultural Works
    """
    LOGGER.info(plot_free_culture.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "ia_cc_totals_by_free_cultural.csv",
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)

    title = "Approved for Free Cultural Works"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(PATHS["data_phase"], "ia_cc_free_culture.png")
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
        "Plots showing Approved for Free Cultural Works legal tool usage"
        " from Internet Archive.",
        "[Understanding Free Cultural Works - Creative"
        " Commons](https://creativecommons.org/public-domain/freeworks/):\n"
        "\n"
        '> Using [the Freedom Defined definition of a "Free Cultural Work"],'
        " material licensed under CC BY or BY-SA is a free cultural work. (So"
        " is anything in the worldwide public domain marked with CC0 or the"
        " Public Domain Mark.) CC's other licenses– BY-NC, BY-ND, BY-NC-SA,"
        " and BY-NC-ND–only allow more limited uses, and material under these"
        " licenses is not considered a free cultural work.",
    )


def plot_open_source_licenses(args):
    """
    Create plots for open source licenses (non-CC)
    """
    LOGGER.info(plot_open_source_licenses.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "ia_open_source_totals.csv",
    )
    
    if not os.path.exists(file_path):
        LOGGER.warning(f"Data file not found: {file_path}")
        return
    
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "License"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values("Count", ascending=False, inplace=True)
    data = data[:10]  # limit to top 10
    data = data[::-1]  # reverse order

    title = "Open Source Licenses (Non-CC)"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "ia_open_source_licenses.png"
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
        "Plots showing open source license usage from Internet Archive"
        " (excluding Creative Commons licenses).",
    )


def main():
    """
    Main function to generate IA reports.
    """
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    ia_intro(args)
    plot_cc_products(args)
    plot_cc_tool_status(args)
    plot_latest_tools(args)
    plot_prior_tools(args)
    plot_retired_tools(args)
    plot_countries_highest_usage(args)
    plot_languages_highest_usage(args)
    plot_free_culture(args)
    plot_open_source_licenses(args)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit Internet Archive (IA) reports for {QUARTER}",
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
