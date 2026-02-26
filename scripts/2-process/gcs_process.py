#!/usr/bin/env python
"""
Process Google Custom Search (GCS) data.
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
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
FILE_PATHS = [
    shared.path_join(PATHS["data_phase"], "gcs_product_totals.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_status_combined_totals.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_status_lastest_totals.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_status_prior_totals.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_status_retired_totals.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_totals_by_country.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_totals_by_free_cultural.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_totals_by_language.csv"),
    shared.path_join(PATHS["data_phase"], "gcs_totals_by_restrictions.csv"),
]


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    global QUARTER
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=QUARTER,
        help=f"Data quarter in format YYYYQx (default: {QUARTER})",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results (default: False)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions such as fetch, merge, add, commit, and push",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate data even if processed files already exist",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        global FILE_PATHS, PATHS
        FILE_PATHS = shared.paths_list_update(
            LOGGER, FILE_PATHS, QUARTER, args.quarter
        )
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
        QUARTER = args.quarter
    args.logger = LOGGER
    args.paths = PATHS
    return args


def process_product_totals(args, count_data):
    """
    Processing count data: totals by product
    """
    LOGGER.info(process_product_totals.__doc__.strip())
    data = {
        "Licenses version 4.0": 0,
        "Licenses version 3.0": 0,
        "Licenses version 2.x": 0,
        "Licenses version 1.0": 0,
        "CC0 1.0": 0,
        "Public Domain Mark 1.0": 0,
        "Certification 1.0 US": 0,
    }
    for row in count_data.itertuples(index=False):
        tool = row[0]
        count = row[1]
        if tool.startswith("PDM"):
            key = "Public Domain Mark 1.0"
        elif "CC0" in tool:
            key = "CC0 1.0"
        elif "PUBLICDOMAIN" in tool:
            key = "Certification 1.0 US"
        elif "4.0" in tool:
            key = "Licenses version 4.0"
        elif "3.0" in tool:
            key = "Licenses version 3.0"
        elif "2." in tool:
            key = "Licenses version 2.x"
        elif "1.0" in tool:
            key = "Licenses version 1.0"
        else:
            raise shared.QuantifyingException("Invalid TOOL_IDENTIFIER")
        data[key] += count

    data = pd.DataFrame(
        data.items(), columns=["CC legal tool product", "Count"]
    )
    file_path = shared.path_join(PATHS["data_phase"], "gcs_product_totals.csv")
    shared.dataframe_to_csv(args, data, file_path)


def process_latest_prior_retired_totals(args, count_data):
    """
    Process count data: totals by unit in three categories: latest, prior,
    and retired
    """
    LOGGER.info(process_latest_prior_retired_totals.__doc__.strip())
    # https://creativecommons.org/retiredlicenses/
    retired = [
        # DevNations,
        "CC DEVNATIONS ",
        # NoDerivs
        "CC ND ",
        # NoDerivs-NonCommercial
        "CC ND-NC ",
        # NonCommercial
        "CC NC ",
        # NonCommercial-Sampling+
        "CC NC-SAMPLING+",
        # NonCommercial-ShareAlike
        "CC NC-SA ",
        # Public Domain Dedication and Certification
        "CC PUBLICDOMAIN",
        # Sampling
        "CC SAMPLING ",
        # Sampling+
        "CC SAMPLING+ ",
        # ShareAlike
        "CC SA ",
    ]
    data = {"latest": {}, "prior": {}, "retired": {}}
    status = {"Latest": 0, "Prior": 0, "Retired": 0}
    for row in count_data.itertuples(index=False):
        tool = row[0]
        count = row[1]
        tool_begin = False
        for version in ["1.0", "2.0", "2.1", "2.5", "3.0", "4.0"]:
            if version in tool:
                separator = tool.index(version)
                # everything before version (including space)
                tool_begin = tool[:separator]
        if not tool_begin:
            tool_begin = tool
        # Latest
        if (
            ("BY" in tool and "4.0" in tool)
            or tool.startswith("CC0")
            or tool.startswith("PDM")
        ):
            try:
                data["latest"][tool] += count
            except KeyError:
                data["latest"][tool] = count
            status["Latest"] += count
        # Prior
        elif "BY" in tool and tool_begin not in retired:
            if "ND-NC" in tool_begin:
                tool_begin = tool_begin.replace("ND-NC", "NC-ND")
            try:
                data["prior"][tool_begin.strip()] += count
            except KeyError:
                data["prior"][tool_begin.strip()] = count
            status["Prior"] += count
        # Retired
        else:
            try:
                data["retired"][tool_begin.strip()] += count
            except KeyError:
                data["retired"][tool_begin.strip()] = count
            status["Retired"] += count
    data["combined"] = status

    for key, value_data in data.items():
        dataframe = pd.DataFrame(
            value_data.items(), columns=["CC legal tool", "Count"]
        )
        file_path = shared.path_join(
            PATHS["data_phase"], f"gcs_status_{key}_totals.csv"
        )
        shared.dataframe_to_csv(args, dataframe, file_path)


def process_totals_by_free_cultural(args, count_data):
    """
    Processing count data: totals by Approved for Free Cultural Works
    """
    # https://creativecommons.org/public-domain/freeworks/
    LOGGER.info(process_totals_by_free_cultural.__doc__.strip())
    data = {
        "Approved for Free Cultural Works": 0,
        "Limited use": 0,
    }
    for row in count_data.itertuples(index=False):
        tool = row[0]
        count = row[1]
        if tool.startswith("PDM") or "CC0" in tool or "PUBLICDOMAIN" in tool:
            key = "Approved for Free Cultural Works"
        else:
            parts = tool.split()
            unit = parts[1].lower()
            if unit in ["by-sa", "by", "sa", "sampling+"]:
                key = "Approved for Free Cultural Works"
            else:
                key = "Limited use"
        data[key] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    data.sort_values("Count", ascending=False, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "gcs_totals_by_free_cultural.csv"
    )
    shared.dataframe_to_csv(args, data, file_path)


def process_totals_by_restrictions(args, count_data):
    """
    Processing count data: totals by restriction
    """
    LOGGER.info(process_totals_by_restrictions.__doc__.strip())
    data = {
        "level 0 - unrestricted": 0,
        "level 1 - few restrictions": 0,
        "level 2 - some restrictions": 0,
        "level 3 - many restrictions": 0,
    }
    for row in count_data.itertuples(index=False):
        tool = row[0]
        count = row[1]
        if tool.startswith("PDM") or "CC0" in tool or "PUBLICDOMAIN" in tool:
            key = "level 0 - unrestricted"
        else:
            parts = tool.split()
            unit = parts[1].lower()
            if unit in ["by-sa", "by", "sa", "sampling+"]:
                key = "level 1 - few restrictions"
            elif unit in ["by-nc", "by-nc-sa", "sampling", "nc", "nc-sa"]:
                key = "level 2 - some restrictions"
            else:
                key = "level 3 - many restrictions"
        data[key] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    file_path = shared.path_join(
        PATHS["data_phase"], "gcs_totals_by_restrictions.csv"
    )
    shared.dataframe_to_csv(args, data, file_path)


def process_totals_by_language(args, data):
    """
    Processing language data: totals by language
    """
    LOGGER.info(process_totals_by_language.__doc__.strip())
    data = data.groupby(["LANGUAGE"], as_index=False)["COUNT"].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "LANGUAGE": "Language",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "gcs_totals_by_language.csv"
    )
    shared.dataframe_to_csv(args, data, file_path)


def process_totals_by_country(args, data):
    """
    Processing country data: totals by country
    """
    LOGGER.info(process_totals_by_country.__doc__.strip())
    data = data.groupby(["COUNTRY"], as_index=False)["COUNT"].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "COUNTRY": "Country",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "gcs_totals_by_country.csv"
    )
    shared.dataframe_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    shared.check_completion_file_exists(args, FILE_PATHS)

    # Count data
    file1_count = shared.path_join(PATHS["data_1-fetch"], "gcs_1_count.csv")
    count_data = shared.open_data_file(
        LOGGER, file1_count, usecols=["TOOL_IDENTIFIER", "COUNT"]
    )
    process_product_totals(args, count_data)
    process_latest_prior_retired_totals(args, count_data)
    process_totals_by_free_cultural(args, count_data)
    process_totals_by_restrictions(args, count_data)

    # Langauge data
    file2_language = shared.path_join(
        PATHS["data_1-fetch"], "gcs_2_count_by_language.csv"
    )
    language_data = shared.open_data_file(
        LOGGER,
        file2_language,
        usecols=["TOOL_IDENTIFIER", "LANGUAGE", "COUNT"],
    )
    process_totals_by_language(args, language_data)

    # Country data
    file3_country = shared.path_join(
        PATHS["data_1-fetch"], "gcs_3_count_by_country.csv"
    )
    country_data = shared.open_data_file(
        LOGGER, file3_country, usecols=["TOOL_IDENTIFIER", "COUNTRY", "COUNT"]
    )
    process_totals_by_country(args, country_data)

    # TODO: compare with previous quarter, previous year

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Google Custom Search (GCS) data for {QUARTER}",
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
