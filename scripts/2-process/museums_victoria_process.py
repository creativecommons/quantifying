#!/usr/bin/env python
"""
Process Google Custom Search (GCS) data.
"""
# Standard library
import argparse
import csv
import json
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
from scripts import shared

# import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
RAW_CSV_FILEPATH = shared.path_join(
    PATHS["data_quarter"], "1-fetch/museums_victoria_raw.csv"
)

DATA_PHASE = PATHS["data_phase"]

REPORT_OVERALL = shared.path_join(DATA_PHASE, 'museums_victoria_overall.csv')
REPORT_BY_DOMAIN = shared.path_join(DATA_PHASE, 'museums_victoria_by_record_type.csv')
REPORT_BY_MEDIA = shared.path_join(DATA_PHASE, 'museums_victoria_by_media_type.csv')
REPORT_BY_RESTRICTION = shared.path_join(DATA_PHASE, 'museums_victoria_by_restriction.csv')


# --- Helper Function for Categorical Analysis (Matching GCS Logic) ---

def get_restriction_level(tool):
    """
    Categorizes a license short name into one of four restriction levels.
    """
    tool = tool.upper().strip()
    if any(keyword in tool for keyword in ["PDM", "CC0", "PUBLICDOMAIN"]):
        return "level 0 - unrestricted"
    if ("BY" in tool or "SA" in tool) and "NC" not in tool and "ND" not in tool:
        return "level 1 - few restrictions"
    elif "NC" in tool or "SAMPLING" in tool:
        return "level 2 - some restrictions"
    else:
        return "level 3 - many restrictions"


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
        default='true'
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions such as fetch, merge, add, commit, and push"
             " (default: False)",
    )
    args = parser.parse_args()
    global PATHS
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
    args.logger = LOGGER
    args.paths = PATHS
    return args


def read_raw_data(args):
    return pd.read_csv(RAW_CSV_FILEPATH)


def normalize_data(args, data):
    """
        Unpacks the 'media_json' column, creating one row per media asset
        with its license and media type.
        """
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    LOGGER.info("Starting data normalization: Unpacking media_json...")
    normalized_rows = []
    for _, row in data.iterrows():
        media_json_string = row['MEDIA JSON']

        if pd.isna(media_json_string) or media_json_string.strip() == '':
            media_list = []
        else:
            try:
                media_list = json.loads(media_json_string)
            except (TypeError, json.JSONDecodeError):
                media_list = []

        record_type = row['RECORD TYPE']

        for media_item in media_list:
            license_short_name = media_item.get('licence', {}).get('shortName', 'Not Found')
            media_type = media_item.get('type', 'Unknown').lower()
            if not license_short_name or license_short_name == 'Not Found':
                license_short_name = 'All Rights Reserved'
            normalized_rows.append({
                'RECORD TYPE': record_type,
                'TOOL IDENTIFIER': license_short_name,
                'MEDIA TYPE': media_type
            })

    df_normalized = pd.DataFrame(normalized_rows)
    LOGGER.info(f"Normalization complete. Total media assets extracted: {len(df_normalized)}")
    return df_normalized


def process_totals_overall(df_normalized):
    """Generates the overall total license count and percentage distribution."""

    df_report = df_normalized.groupby('TOOL IDENTIFIER').size().reset_index(name='TOTAL COUNT')
    total_count = df_report['TOTAL COUNT'].sum()
    df_report['PERCENTAGE'] = (df_report['TOTAL COUNT'] / total_count) * 100

    df_report.sort_values(by='TOTAL COUNT', ascending=False, inplace=True)
    df_report.reset_index(drop=True, inplace=True)
    df_report.index = df_report.index + 1
    df_report['PLAN INDEX'] = df_report.index

    df_report = df_report[['PLAN INDEX', 'TOOL IDENTIFIER', 'TOTAL COUNT', 'PERCENTAGE']]
    df_report.to_csv(REPORT_OVERALL, index=False)
    return df_report


def process_totals_by_domain(df_normalized):
    """Generates license counts grouped by RECORD_TYPE (Domain/Endeavor proxy)."""
    df_report = df_normalized.groupby(['RECORD TYPE', 'TOOL IDENTIFIER']).size().reset_index(name='COUNT')

    df_report['PROPORTION OF DOMAIN'] = df_report.groupby('RECORD TYPE')['COUNT'].transform(lambda x: x / x.sum())

    df_report.sort_values(by=['RECORD TYPE', 'COUNT'], ascending=[True, False], inplace=True)
    df_report.reset_index(drop=True, inplace=True)
    df_report.index = df_report.index + 1
    df_report['PLAN INDEX'] = df_report.index

    df_report = df_report[['PLAN INDEX', 'RECORD TYPE', 'TOOL IDENTIFIER', 'COUNT', 'PROPORTION OF DOMAIN']]
    df_report.to_csv(REPORT_BY_DOMAIN, index=False)
    return df_report


def process_totals_by_media_type(df_normalized):
    """Generates license counts grouped by MEDIA_TYPE (Image, Audio, Video)."""

    # Group by MEDIA_TYPE and TOOL_IDENTIFIER
    df_report = df_normalized.groupby(['MEDIA TYPE', 'TOOL IDENTIFIER']).size().reset_index(name='COUNT')
    df_report['PROPORTION OF TYPE'] = df_report.groupby('MEDIA TYPE')['COUNT'].transform(lambda x: x / x.sum())

    df_report.sort_values(by=['MEDIA TYPE', 'COUNT'], ascending=[True, False], inplace=True)
    df_report.reset_index(drop=True, inplace=True)
    df_report.index = df_report.index + 1
    df_report['PLAN INDEX'] = df_report.index

    df_report = df_report[['PLAN INDEX', 'MEDIA TYPE', 'TOOL IDENTIFIER', 'COUNT', 'PROPORTION OF TYPE']]
    df_report.to_csv(REPORT_BY_MEDIA, index=False)
    return df_report


def process_totals_by_restriction(df_normalized):
    """
    Generates totals grouped by license restriction level (GCS Analogue).
    """
    # Add the restriction level column
    df_normalized['RESTRICTION LEVEL'] = df_normalized['TOOL IDENTIFIER'].apply(get_restriction_level)

    df_report = df_normalized.groupby('RESTRICTION LEVEL').size().reset_index(name='COUNT')
    total_count = df_report['COUNT'].sum()
    df_report['PERCENTAGE'] = (df_report['COUNT'] / total_count) * 100

    # Sort explicitly by restriction level for logical flow
    level_order = ["level 0 - unrestricted", "level 1 - few restrictions", "level 2 - some restrictions",
                   "level 3 - many restrictions"]
    df_report['RESTRICTION LEVEL'] = pd.Categorical(df_report['RESTRICTION LEVEL'], categories=level_order,
                                                    ordered=True)
    df_report.sort_values("RESTRICTION LEVEL", inplace=True)

    df_report.reset_index(drop=True, inplace=True)
    df_report.index = df_report.index + 1
    df_report['PLAN INDEX'] = df_report.index

    df_report = df_report[['PLAN INDEX', 'RESTRICTION LEVEL', 'COUNT', 'PERCENTAGE']]
    df_report.to_csv(REPORT_BY_RESTRICTION, index=False)
    return df_report


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    # shared.git_fetch_and_merge(args, PATHS["repo"])

    # Count data
    raw_data = read_raw_data(args)
    normalized_data = normalize_data(args, raw_data)
    process_totals_overall(normalized_data)
    process_totals_by_media_type(normalized_data)
    process_totals_by_domain(normalized_data)
    process_totals_by_restriction(normalized_data)
    # args = shared.git_add_and_commit(
    #     args,
    #     PATHS["repo"],
    #     PATHS["data_quarter"],
    #     f"Add and commit new Google Custom Search (GCS) data for {QUARTER}",
    # )
    # shared.git_push_changes(args, PATHS["repo"])


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
