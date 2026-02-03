#!/usr/bin/env python
# Standard library
import csv
import os
import sys
import textwrap
import traceback
from copy import deepcopy
from types import SimpleNamespace

# Third-party
import yaml
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add current directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Log the start of the script execution
LOGGER.info("Script execution started")


def sort_tools(tool):
    tool.ver_sort = 999 - int(float(tool.version) * 10)
    if tool.jurisdiction == "":
        tool.jur_sort = "A"
    else:
        tool.jur_sort = tool.jurisdiction
    # Priority 1: 4.0 licenses
    if tool.category == "licenses" and tool.version == "4.0":
        priority = 1
    # Priority 2: CC0 and PDM
    elif tool.category == "publicdomain" and tool.unit in ("mark", "zero"):
        priority = 2
    # Priority 3: unported 1.0-3.0 by* licenses
    elif (
        tool.category == "licenses"
        and tool.unit.startswith("by")
        and tool.version != ""
        and tool.jurisdiction == ""
    ):
        priority = 3
    # Priority 4: ported 1.0-3.0 by* licenses
    elif (
        tool.category == "licenses"
        and tool.unit.startswith("by")
        and tool.version != ""
        and tool.jurisdiction != ""
    ):
        priority = 4
    # Priority 5: unported 1.0-3.0 non-by* licenses
    elif (
        tool.category == "licenses"
        and not tool.unit.startswith("by")
        and tool.version != ""
        and tool.jurisdiction == ""
    ):
        priority = 5
    # Priority 6: ported 1.0-3.0 non-by* licenses
    elif (
        tool.category == "licenses"
        and not tool.unit.startswith("by")
        and tool.version != ""
        and tool.jurisdiction != ""
    ):
        priority = 6
    # Priority 7: miscellaneous
    else:
        priority = 7
    return f"{priority}-{tool.ver_sort}-{tool.jur_sort}-{tool.unit}"


def get_tools_metadata_namespace():
    LOGGER.info("Loading CC Legal Tool metadata")
    file_path = shared.path_join(PATHS["data"], "cc-legal-tools.csv")
    tools_metadata = []
    with open(file_path, "r", encoding="utf-8") as file_obj:
        rows = csv.DictReader(file_obj, dialect="unix")
        for row in rows:
            tool = SimpleNamespace()
            for key, value in row.items():
                setattr(tool, key.lower(), value)
            tool.canonical_url = tool.canonical_url.replace("https:", "")
            tool.canonical_url = tool.canonical_url.rstrip("/")
            tools_metadata.append(tool)
            # Add tool with legacy URL for CERTIFICATION 1.0 US
            if tool.identifier == "CERTIFICATION 1.0 US":
                legacy_tool = deepcopy(tool)
                legacy_tool.canonical_url = (
                    "//creativecommons.org/licenses/publicdomain"
                )
                tools_metadata.append(legacy_tool)
    LOGGER.info("Prioritizing CC Legal Tool metadata entries")
    tools_metadata.sort(key=sort_tools)
    return tools_metadata


def load_countries():
    file_path = shared.path_join(PATHS["data"], "gcs_country_collection.yaml")
    with open(file_path, "r", encoding="utf-8") as file_obj:
        countries = yaml.safe_load(file_obj)
    return countries


def load_languages():
    file_path = shared.path_join(PATHS["data"], "gcs_language_collection.yaml")
    with open(file_path, "r", encoding="utf-8") as file_obj:
        languages = yaml.safe_load(file_obj)
    return languages


def create_query_plan(tools_metadata, countries, languages):
    plan = []
    # ideal: all tools, all countries, all languages: 5,522,440

    # cr: Google Country Collection value
    # lr: Google Language Collection value

    # Group 1: All tools (without country or language) is 640
    for tool in tools_metadata:
        plan.append(
            {
                "TOOL_URL": tool.canonical_url,
                "TOOL_IDENTIFIER": tool.identifier,
            }
        )

    # Group 2: 4.0 licenses (6) by language (35) is 210
    #          CC0 (1) by language (35) .........is  35
    #          PDM (1) by language (35) .........is  35
    #          ......................... ..subtotal 280
    for tool in tools_metadata:
        if (
            tool.category == "licenses" and tool.version == "4.0"
        ) or tool.unit in ("mark", "zero"):
            for pair in languages:
                plan.append(
                    {
                        "TOOL_URL": tool.canonical_url,
                        "TOOL_IDENTIFIER": tool.identifier,
                        "LANGUAGE": pair["language"],
                        "LR": pair["lr"],
                    }
                )

    # Group 3: 4.0 licenses (6) by country (242) is 1,452
    #          CC0 (1) by country (242)..........is   242
    #          PDM (1) by country (242)..........is   242
    #          ............................subtotal 1,936
    for tool in tools_metadata:
        if (
            tool.category == "licenses" and tool.version == "4.0"
        ) or tool.unit in ("mark", "zero"):
            for pair in countries:
                plan.append(
                    {
                        "TOOL_URL": tool.canonical_url,
                        "TOOL_IDENTIFIER": tool.identifier,
                        "COUNTRY": pair["country"],
                        "CR": pair["cr"],
                    }
                )

    # plan total: 2,856
    LOGGER.info(f"Plan entries: {len(plan)}")
    return plan


def save_plan(plan):
    LOGGER.info("Saving Google query plan to CSV")
    file_path = shared.path_join(PATHS["data"], "gcs_query_plan.csv")
    fieldnames = [
        "TOOL_URL",
        "TOOL_IDENTIFIER",
        "COUNTRY",
        "CR",
        "LANGUAGE",
        "LR",
    ]
    with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=fieldnames, dialect="unix"
        )
        writer.writeheader()
        for row in plan:
            writer.writerow(row)


def main():
    tools_metadata = get_tools_metadata_namespace()
    countries = load_countries()
    languages = load_languages()
    plan = create_query_plan(tools_metadata, countries, languages)
    save_plan(plan)


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
