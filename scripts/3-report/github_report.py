#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected
from GitHub.
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
SECTION = "GitHub data"


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


def load_data(args):
    """
    Load the collected data from the CSV file.
    """
    selected_quarter = args.quarter

    file_path = os.path.join(
        PATHS["data"], f"{selected_quarter}", "1-fetch", "github_1_count.csv"
    )

    if not os.path.exists(file_path):
        LOGGER.error(f"Data file not found: {file_path}")
        return pd.DataFrame()

    data = pd.read_csv(file_path)
    LOGGER.info(f"Data loaded from {file_path}")
    return data


def github_intro(args):
    """
    Write GitHub introduction.
    """
    LOGGER.info(github_intro.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_1-fetch"],
        "github_1_count.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "TOOL_IDENTIFIER"
    data = pd.read_csv(file_path, index_col=name_label)
    total_repositories = data.loc["Total public repositories", "COUNT"]
    cc_total = data[data.index.str.startswith("CC")]["COUNT"].sum()
    cc_percentage = f"{(cc_total / total_repositories) * 100:.2f}%"
    shared.update_readme(
        args,
        SECTION,
        "Overview",
        None,
        None,
        "The GitHub data, below, uses the `total_count`"
        " returned by API for search queries of the various legal tools."
        "\n"
        f"**The results indicate that {cc_total} ({cc_percentage})"
        f"** of the {total_repositories} total public repositories"
        " on GitHub that use a CC legal tool. Additionally,"
        " many more use a non-CC use a Public domain"
        " equivalent legal tools.**\n"
        "\n"
        " The Github data showcases the different level of"
        " rights reserved on repositories We have Public"
        " domain which includes works released under CC0, 0BSD and Unlicense"
        " meaning developers have waived all their rights to a software."
        " Allowing anyone to freely use, modify, and distribute the code"
        " without restriction."
        " See more at"
        " [Public-domain-equivalent license]"
        "(https://en.wikipedia.org/wiki/Public-domain-equivalent_license)"
        " While a Permissive category of license contains works"
        " under MIT-0 and CC BY 4.0 allows users to"
        " reuse the code with some conditions and attribution"
        " [Permissive license]"
        "(https://en.wikipedia.org/wiki/Permissive_software_license)"
        " and Copyleft contains works under CC BY-SA 4.0."
        " which requires any derivative works to be licensed"
        " under the same terms."
        " [Copyleft](https://en.wikipedia.org/wiki/Copyleft)"
        "\n"
        "Thank you GitHub for providing public API"
        " access to repository metadata!",
    )


def plot_totals_by_license_type(args):
    """
    Create plots showing totals by license type
    """
    LOGGER.info(plot_totals_by_license_type.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "github_totals_by_license.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "License"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)
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
        PATHS["data_phase"], "github_totals_by_license_type.png"
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
        "Plots showing totals by license type."
        " This shows the distribution of different CC license"
        " and non CC license used in GitHub repositories."
        " Allowing Commons to evaluate how freely softwares on"
        " GitHub are being used, modified, and shared"
        " and how developers choose to share their works."
        " See more at [SPDX License List]"
        "(https://spdx.org/licenses/)",
    )


def plot_totals_by_restriction(args):
    """
    Create plots showing totals by restriction
    """
    LOGGER.info(plot_totals_by_restriction.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "github_totals_by_restriction.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "Category"
    data_label = "Count"
    data = pd.read_csv(file_path, index_col=name_label)
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
        PATHS["data_phase"], "github_restriction.png"
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
        "Plots showing totals by different levels of restrictions."
        " This shows the distribution of Public domain,"
        " Permissive, and Copyleft"
        " licenses used in GitHub repositories.",
    )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    github_intro(args)
    plot_totals_by_license_type(args)
    plot_totals_by_restriction(args)

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
