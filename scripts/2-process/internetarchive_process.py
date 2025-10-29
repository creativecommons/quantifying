#!/usr/bin/env python
"""
Process Internet Archive (IA) data.
"""
# Standard library
import argparse
import csv
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


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
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


def data_to_csv(args, data, file_path):
    """
    Save data to CSV file.
    """
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)
    # emulate csv.unix_dialect
    data.to_csv(
        file_path, index=False, quoting=csv.QUOTE_ALL, lineterminator="\n"
    )


def process_license_totals(args, count_data):
    """
    Processing count data: totals by normalized license
    """
    LOGGER.info(process_license_totals.__doc__.strip())
    
    # Group by normalized license and sum counts
    data = count_data.groupby(["NORMALIZED_LICENSE"], as_index=False)["COUNT"].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    
    file_path = shared.path_join(PATHS["data_phase"], "ia_license_totals.csv")
    data_to_csv(args, data, file_path)


def process_cc_license_totals(args, count_data):
    """
    Processing count data: totals by CC license categories
    """
    LOGGER.info(process_cc_license_totals.__doc__.strip())
    
    # Filter for Creative Commons licenses only
    cc_data = count_data[count_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    # Categorize by license type
    data = {
        "Licenses version 4.0": 0,
        "Licenses version 3.0": 0,
        "Licenses version 2.x": 0,
        "Licenses version 1.0": 0,
        "CC0 1.0": 0,
        "Public Domain Mark 1.0": 0,
        "Other CC licenses": 0,
    }
    
    for row in cc_data.itertuples(index=False):
        license_name = row[1]  # NORMALIZED_LICENSE
        count = row[2]  # COUNT
        
        if license_name.startswith("PDM"):
            key = "Public Domain Mark 1.0"
        elif "CC0" in license_name:
            key = "CC0 1.0"
        elif "4.0" in license_name:
            key = "Licenses version 4.0"
        elif "3.0" in license_name:
            key = "Licenses version 3.0"
        elif "2." in license_name:
            key = "Licenses version 2.x"
        elif "1.0" in license_name:
            key = "Licenses version 1.0"
        else:
            key = "Other CC licenses"
        
        data[key] += count

    data = pd.DataFrame(
        data.items(), columns=["CC legal tool product", "Count"]
    )
    file_path = shared.path_join(PATHS["data_phase"], "ia_cc_product_totals.csv")
    data_to_csv(args, data, file_path)


def process_latest_prior_retired_totals(args, count_data):
    """
    Process count data: totals by unit in three categories: latest, prior,
    and retired for CC licenses
    """
    LOGGER.info(process_latest_prior_retired_totals.__doc__.strip())
    
    # Filter for Creative Commons licenses only
    cc_data = count_data[count_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    # https://creativecommons.org/retiredlicenses/
    retired = [
        "CC DEVNATIONS ",
        "CC ND ",
        "CC ND-NC ",
        "CC NC ",
        "CC NC-SAMPLING+",
        "CC NC-SA ",
        "CC PUBLICDOMAIN",
        "CC SAMPLING ",
        "CC SAMPLING+ ",
        "CC SA ",
    ]
    
    data = {"latest": {}, "prior": {}, "retired": {}}
    status = {"Latest": 0, "Prior": 0, "Retired": 0}
    
    for row in cc_data.itertuples(index=False):
        license_name = row[1]  # NORMALIZED_LICENSE
        count = row[2]  # COUNT
        
        tool_begin = False
        for version in ["1.0", "2.0", "2.1", "2.5", "3.0", "4.0"]:
            if version in license_name:
                separator = license_name.index(version)
                # everything before version (including space)
                tool_begin = license_name[:separator]
        
        if not tool_begin:
            tool_begin = license_name
        
        # Latest
        if (
            ("BY" in license_name and "4.0" in license_name)
            or license_name.startswith("CC0")
            or license_name.startswith("PDM")
        ):
            try:
                data["latest"][license_name] += count
            except KeyError:
                data["latest"][license_name] = count
            status["Latest"] += count
        # Prior
        elif "BY" in license_name and tool_begin not in retired:
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
            PATHS["data_phase"], f"ia_cc_status_{key}_totals.csv"
        )
        data_to_csv(args, dataframe, file_path)


def process_totals_by_free_cultural(args, count_data):
    """
    Processing count data: totals by Approved for Free Cultural Works
    """
    LOGGER.info(process_totals_by_free_cultural.__doc__.strip())
    
    # Filter for Creative Commons licenses only
    cc_data = count_data[count_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    data = {
        "Approved for Free Cultural Works": 0,
        "Limited use": 0,
    }
    
    for row in cc_data.itertuples(index=False):
        license_name = row[1]  # NORMALIZED_LICENSE
        count = row[2]  # COUNT
        
        if license_name.startswith("PDM") or "CC0" in license_name or "PUBLICDOMAIN" in license_name:
            key = "Approved for Free Cultural Works"
        else:
            parts = license_name.split()
            if len(parts) > 1:
                unit = parts[1].lower()
                if unit in ["by-sa", "by", "sa", "sampling+"]:
                    key = "Approved for Free Cultural Works"
                else:
                    key = "Limited use"
            else:
                key = "Limited use"
        
        data[key] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    data.sort_values("Count", ascending=False, inplace=True)
    data.reset_index(drop=True, inplace=True)
    file_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_totals_by_free_cultural.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_restrictions(args, count_data):
    """
    Processing count data: totals by restriction level
    """
    LOGGER.info(process_totals_by_restrictions.__doc__.strip())
    
    # Filter for Creative Commons licenses only
    cc_data = count_data[count_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    data = {
        "level 0 - unrestricted": 0,
        "level 1 - few restrictions": 0,
        "level 2 - some restrictions": 0,
        "level 3 - many restrictions": 0,
    }
    
    for row in cc_data.itertuples(index=False):
        license_name = row[1]  # NORMALIZED_LICENSE
        count = row[2]  # COUNT
        
        if license_name.startswith("PDM") or "CC0" in license_name or "PUBLICDOMAIN" in license_name:
            key = "level 0 - unrestricted"
        else:
            parts = license_name.split()
            if len(parts) > 1:
                unit = parts[1].lower()
                if unit in ["by-sa", "by", "sa", "sampling+"]:
                    key = "level 1 - few restrictions"
                elif unit in ["by-nc", "by-nc-sa", "sampling", "nc", "nc-sa"]:
                    key = "level 2 - some restrictions"
                else:
                    key = "level 3 - many restrictions"
            else:
                key = "level 3 - many restrictions"
        
        data[key] += count

    data = pd.DataFrame(data.items(), columns=["Category", "Count"])
    file_path = shared.path_join(
        PATHS["data_phase"], "ia_cc_totals_by_restrictions.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_language(args, language_data):
    """
    Processing language data: totals by language
    """
    LOGGER.info(process_totals_by_language.__doc__.strip())
    
    # Filter for Creative Commons licenses only
    cc_language_data = language_data[language_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    data = cc_language_data.groupby(["LANGUAGE"], as_index=False)["COUNT"].sum()
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
        PATHS["data_phase"], "ia_cc_totals_by_language.csv"
    )
    data_to_csv(args, data, file_path)


def process_totals_by_country(args, country_data):
    """
    Processing country data: totals by country
    """
    LOGGER.info(process_totals_by_country.__doc__.strip())
    
    # Filter for Creative Commons licenses only
    cc_country_data = country_data[country_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    data = cc_country_data.groupby(["COUNTRY"], as_index=False)["COUNT"].sum()
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
        PATHS["data_phase"], "ia_cc_totals_by_country.csv"
    )
    data_to_csv(args, data, file_path)


def process_media_type_totals(args, count_data):
    """
    Processing count data: totals by media type
    """
    LOGGER.info(process_media_type_totals.__doc__.strip())
    
    # This would require media type data from the fetch phase
    # For now, we'll create a placeholder
    data = pd.DataFrame({
        "Media Type": ["texts", "audio", "video", "image", "software", "other"],
        "Count": [0, 0, 0, 0, 0, 0]
    })
    
    file_path = shared.path_join(
        PATHS["data_phase"], "ia_media_type_totals.csv"
    )
    data_to_csv(args, data, file_path)


def process_open_source_totals(args, count_data):
    """
    Processing count data: totals by open source licenses (non-CC)
    """
    LOGGER.info(process_open_source_totals.__doc__.strip())
    
    # Filter for non-CC licenses
    open_source_data = count_data[~count_data["NORMALIZED_LICENSE"].str.contains("CC", na=False)]
    
    data = open_source_data.groupby(["NORMALIZED_LICENSE"], as_index=False)["COUNT"].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "NORMALIZED_LICENSE": "License",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "ia_open_source_totals.csv"
    )
    data_to_csv(args, data, file_path)


def main():
    """
    Main function to process IA data.
    """
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    # Count data
    file1_count = shared.path_join(PATHS["data_1-fetch"], "ia_1_count.csv")
    if not os.path.exists(file1_count):
        LOGGER.error(f"Count data file not found: {file1_count}")
        return
    
    count_data = pd.read_csv(file1_count, usecols=["LICENSE_URL", "NORMALIZED_LICENSE", "COUNT"])
    
    # Process various aggregations
    process_license_totals(args, count_data)
    process_cc_license_totals(args, count_data)
    process_latest_prior_retired_totals(args, count_data)
    process_totals_by_free_cultural(args, count_data)
    process_totals_by_restrictions(args, count_data)
    process_open_source_totals(args, count_data)
    process_media_type_totals(args, count_data)

    # Language data
    file2_language = shared.path_join(
        PATHS["data_1-fetch"], "ia_2_count_by_language.csv"
    )
    if os.path.exists(file2_language):
        language_data = pd.read_csv(
            file2_language, usecols=["LICENSE_URL", "NORMALIZED_LICENSE", "LANGUAGE", "COUNT"]
        )
        process_totals_by_language(args, language_data)

    # Country data
    file3_country = shared.path_join(
        PATHS["data_1-fetch"], "ia_3_count_by_country.csv"
    )
    if os.path.exists(file3_country):
        country_data = pd.read_csv(
            file3_country, usecols=["LICENSE_URL", "NORMALIZED_LICENSE", "COUNTRY", "COUNT"]
        )
        process_totals_by_country(args, country_data)

    # Git operations
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit processed Internet Archive (IA) data for {QUARTER}",
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

