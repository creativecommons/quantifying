#!/usr/bin/env python
# Standard library
import csv
import os
import sys
import textwrap
import traceback
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


def assign_tool_parts(url):
    tool = SimpleNamespace()
    tool.url = url
    dirs = url.strip().split("/")[3:]
    tool.category = dirs[0]
    tool.unit = dirs[1]
    if len(dirs) > 2:
        tool.version = dirs[2]
        tool.ver_sort = 999 - int(float(dirs[2]) * 10)
    else:
        tool.version = None
        tool.ver_sort = 999
    if len(dirs) == 4:
        tool.jurisdiction = dirs[3]
        tool.jur_sort = dirs[3]
    else:
        tool.jurisdiction = None
        tool.jur_sort = "A"

    # Identifier code is based on CC Legal Tools application:
    # https://github.com/creativecommons/cc-legal-tools-app/blob/c3ac573871c7e20517539851de16998307f20d78/legal_tools/models.py#L677-L694
    if tool.version is not None:
        tool.identifier = f"{tool.unit} {tool.version}"
    else:
        tool.identifier = f"{tool.unit}"

    if tool.unit == "mark":
        tool.identifier = f"PDM {tool.version}"
    elif tool.unit == "zero":
        tool.identifier = f"CC0 {tool.version}"
    elif tool.category == "licenses":
        tool.identifier = f"CC {tool.identifier}"

    if tool.jurisdiction:
        tool.identifier = f"{tool.identifier} {tool.jurisdiction}"
    tool.identifier = tool.identifier.upper()

    return tool


def sort_tools(url):
    tool = assign_tool_parts(url)
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
        and tool.version is not None
        and tool.jurisdiction is None
    ):
        priority = 3
    # Priority 4: ported 1.0-3.0 by* licenses
    elif (
        tool.category == "licenses"
        and tool.unit.startswith("by")
        and tool.version is not None
        and tool.jurisdiction is not None
    ):
        priority = 4
    # Priority 5: unported 1.0-3.0 non-by* licenses
    elif (
        tool.category == "licenses"
        and not tool.unit.startswith("by")
        and tool.version is not None
        and tool.jurisdiction is None
    ):
        priority = 5
    # Priority 6: ported 1.0-3.0 non-by* licenses
    elif (
        tool.category == "licenses"
        and not tool.unit.startswith("by")
        and tool.version is not None
        and tool.jurisdiction is not None
    ):
        priority = 6
    # Priority 7: miscellaneous
    else:
        priority = 7
    return f"{priority}-{tool.ver_sort}-{tool.jur_sort}-{tool.unit}"


def get_tool_urls():
    LOGGER.info("Loading CC Legal Tool paths and adding prefix")
    file_path = shared.path_join(PATHS["data"], "legal-tool-paths.txt")
    prefix = "//creativecommons.org/"
    tool_urls = []
    with open(file_path, "r", encoding="utf-8") as file_obj:
        for line in file_obj:
            tool_urls.append(f"{prefix}{line.strip()}")
    LOGGER.info("Prioritizing CC Legal Tool URLs")
    tool_urls.sort(key=sort_tools)
    return tool_urls


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


def create_query_plan(tool_urls, countries, languages):
    tool_data = {}
    for url in tool_urls:
        tool = assign_tool_parts(url)
        tool_data[tool.identifier] = tool

    plan = []

    # ideal: all tools, all countries, all languages: 5,522,440

    # cr: Google Country Collection value
    # lr: Google Language Collection value

    # Group 1: All tools without cr or lr
    #           subtotal:    652
    for identifier, tool in tool_data.items():
        plan.append({"TOOL_URL": tool.url, "TOOL_IDENTIFIER": identifier})

    # Group 2: 4.0 licenses (6) by language (35)
    #          CC0 (1) by language (35)
    #          PDM (1) by language (35)
    #          subtotal:    280
    for identifier, tool in tool_data.items():
        if (
            tool.category == "licenses" and tool.version == "4.0"
        ) or tool.unit in ("mark", "zero"):
            for pair in languages:
                plan.append(
                    {
                        "TOOL_URL": tool.url,
                        "TOOL_IDENTIFIER": identifier,
                        "LANGUAGE": pair["language"],
                        "LR": pair["lr"],
                    }
                )

    # Group 3: 4.0 licenses (6) by country (242)
    #          CC0 (1) by country (242)
    #          PDM (1) by country (242)
    #          subtotal: 1,936
    for identifier, tool in tool_data.items():
        if (
            tool.category == "licenses" and tool.version == "4.0"
        ) or tool.unit in ("mark", "zero"):
            for pair in countries:
                plan.append(
                    {
                        "TOOL_URL": tool.url,
                        "TOOL_IDENTIFIER": identifier,
                        "COUNTRY": pair["country"],
                        "CR": pair["cr"],
                    }
                )

    # plan total: 2,868
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
    tool_urls = get_tool_urls()
    countries = load_countries()
    languages = load_languages()
    plan = create_query_plan(tool_urls, countries, languages)
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
