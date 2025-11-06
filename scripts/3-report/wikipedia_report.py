#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from Wikipedia.
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
SECTION = "Wikipedia data"


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


def wikipedia_intro(args):
    """
    Write Wikipedia introduction.
    """
    LOGGER.info(wikipedia_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_1-fetch"],
        "wikipedia_count_by_languages.csv",
    )
    LOGGER.info(f"data file:" f"{file_path.replace(PATHS['repo'], '.')}")
    file_path_top10 = shared.path_join(
        PATHS["data_2-process"],
        "wikipedia_highest_language_usage.csv",
    )
    LOGGER.info(
        f"Data file (top 10 languages):"
        f"{file_path_top10.replace(PATHS['repo'], '.')}"
    )
    name_label = "LANGUAGE_NAME_EN"
    name_label_top10 = "language_name_en"
    data = pd.read_csv(file_path, index_col=name_label)
    total_articles = data["COUNT"].sum()
    top10 = pd.read_csv(file_path_top10, index_col=name_label_top10)
    top10_articles = top10["count"].sum()
    top10_percentage = (top10_articles / total_articles) * 100
    average_articles = total_articles / len(data)
    language_count = len(data)
    shared.update_readme(
        args,
        SECTION,
        "Overview",
        None,
        None,
        "This report provides insights into the usage"
        " of the Creative Commons sharelike 4.0 across"
        " the different language edition of Wikipedia."
        " The wikipedia data, below, uses the `count`"
        " field from the Wikipedia API to quantify the number of articles"
        " in each language edition "
        " of Wikipedia."
        f"** The total number of Wikipedia articles across"
        f"** {language_count} languages is"
        f"** {total_articles:,}. The top 10 languages account for"
        f"** {top10_articles:,} articles, which is"
        f"** {top10_percentage:.2f}% of the total articles."
        f"** The average number of articles per language is"
        f"** {average_articles:,.2f}.**"
        "\n"
        "Thank you to Wikipedia and the Wikimedia Foundation for"
        " making this data publicly available!",
    )


def plot_highest_language_usage(args):
    """
    Create plots showing totals by license type
    """
    LOGGER.info(plot_highest_language_usage.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "wikipedia_highest_language_usage.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "language_name_en"
    data_label = "count"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    title = "Top 10 Highest Language Usage"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_highest_language_usage.png"
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
        "Plots showing the top 10 highest language usage"
        " across different language editions of Wikipedia."
        " This shows which languages have the most articles under CC BY-SA 4.0"
        " in Wikipedia, highlighting the distribution of content"
        " across languages.",
    )


def plot_language_representation(args):
    """
    Create plots showing language representation
    """
    LOGGER.info(plot_language_representation.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "wikipedia_language_representation.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "category"
    data_label = "language_count"
    data = pd.read_csv(file_path, index_col=name_label)
    data.sort_values(data_label, ascending=True, inplace=True)
    title = "Language Representation"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "wikipedia_language_representation.png"
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
        "Plots showing the language representation"
        " across different language editions of Wikipedia."
        " This shows how many languages are underrepresented"
        " (below average number of articles) versus"
        " represented (above average number of articles).",
    )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    wikipedia_intro(args)
    plot_highest_language_usage(args)
    plot_language_representation(args)

    # Add and commit changes
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit Wikipedia report for {QUARTER}",
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
