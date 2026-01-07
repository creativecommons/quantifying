#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from Openverse.
"""
# Standard library
import argparse
import os
import sys
import textwrap
import traceback

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
SECTION = "3-openverse_report.py"


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


def openverse_intro(args):
    """
    Write Openverse Introduction.
    """
    LOGGER.info(openverse_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_1-fetch"],
        "openverse_fetch.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "TOOL_IDENTIFIER"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    total = data["MEDIA_COUNT"].sum()
    media_counts = data.groupby("MEDIA_TYPE")["MEDIA_COUNT"].sum()
    total_media = media_counts.sum()
    audio_percentage = (
        f"{(media_counts.get('audio', 0) / total_media) * 100:.2f}"
    )
    images_percentage = (
        f"{(media_counts.get('images', 0) / total_media) * 100:.2f}"
    )
    unique_sources = data["SOURCE"].nunique()
    shared.update_readme(
        args,
        SECTION,
        "Overview",
        None,
        None,
        "The Openverse data, below, uses the `Media_count field`"
        " returned by API for search queries of the various legal tools."
        "\n"
        f" The results indicate that there are {total} count of audio"
        " and images that are licensed or put in the"
        " public domain using a Creative Commons (CC) legal tool."
        " They respectively take a percentage of"
        f" {audio_percentage} and {images_percentage},"
        " of the total media count returned by the Openverse API."
        "\n"
        f"There are {unique_sources} count of"
        f" data sources under the openverse API.\n"
        "\n"
        "Thank you Openverse for providing a public API"
        " access to its media metadata!",
    )


def plot_totals_by_license_type(args):
    """
    Create plots showing totals by license type
    """
    LOGGER.info(plot_totals_by_license_type.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "openverse_totals_by_license.csv",
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
        PATHS["data_phase"], "openverse_totals_by_license_type.png"
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
        "Plots showing Creative Commons (CC) legal tool totals and"
        " percentages.",
    )


def plot_totals_by_media_type(args):
    """
    Create plots showing totals by media type
    """
    LOGGER.info(plot_totals_by_media_type.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "openverse_totals_by_media_type.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Media_type"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)
    title = "Totals by media_type"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "openverse_totals_by_media_type.png"
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
        "Plots showing Creative Commons (CC) legal tool"
        " totals by each media type",
    )


def plot_totals_by_sources(args):
    """
    Create plots showing totals by sources
    """
    LOGGER.info(plot_totals_by_sources.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "openverse_totals_by_sources.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Source"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)
    top_10 = data.head(10)
    title = "Totals by sources"
    plt = plot.combined_plot(
        args=args,
        data=top_10,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(PATHS["data_phase"], "openverse_sources.png")
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
        "Plots showing Creative Commons (CC) legal tool totals"
        " across the top 10 sources returned by openverse API.",
    )


def plot_permissive_by_media_type(args):
    """
    Create plots showing the count of permissive content by media type
    """
    LOGGER.info(plot_permissive_by_media_type.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "openverse_permissive_by_media_type.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Media_type"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)
    title = "Permissive content by media type"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "openverse_permissive_by_media_type.png"
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
        "Plots showing count of permissive content by media type.",
    )


def plot_permissive_by_source(args):
    """
    Create plots showing count of permissive content by source
    """
    LOGGER.info(plot_permissive_by_source.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "openverse_permissive_by_source.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Source"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(name_label, ascending=True, inplace=True)
    top_10 = data.head(10)
    title = "Permissive by source"
    plt = plot.combined_plot(
        args=args,
        data=top_10,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "openverse_permissive_by_source.png"
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
        "Plots showing count of permissive content"
        " by top 10 sources in openverse.",
    )


def plot_totals_by_restriction(args):
    """
    Create plots showing totals by restriction
    """
    LOGGER.info(plot_totals_by_restriction.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "openverse_totals_by_restriction.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = shared.open_data_file(LOGGER, file_path, index_col=name_label)
    data.sort_values(name_label, ascending=False, inplace=True)
    title = "Totals by restriction"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label=data_label,
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "openverse_restriction.png"
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
        "Plots showing totals by different levels of rights reserved"
        " on openverse media contents."
        " This shows the distribution of Public domain,"
        " Permissive, Copyleft and restricted"
        " licenses used in Openverse media contents.",
    )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    openverse_intro(args)
    plot_totals_by_license_type(args)
    plot_totals_by_media_type(args)
    plot_permissive_by_media_type(args)
    plot_permissive_by_source(args)
    plot_totals_by_restriction(args)

    # Add and commit changes
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit Openverse reports for {QUARTER}",
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
