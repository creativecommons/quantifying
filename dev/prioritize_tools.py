#!/usr/bin/env python
"""
Create a prioritized CC Legal Tool URLs file:
1. Load CC Legal Tool paths
2. Add prefix (protocol and domain)
3. Prioritize CC Legal Tool URLs
   1. Priority 1: 4.0 licenses
   2. Priority 2: CC0 and PDM
   3. Priority 3: unported 1.0-3.0 by* licenses
   4. Priority 4: ported 1.0-3.0 by* licenses
   5. Priority 5: unported 1.0-3.0 non-by* licenses
   6. Priority 6: ported 1.0-3.0 non-by* licenses
   7. Priority 7: miscellaneous
4. Save prioritized CC Legal Tool URLs
"""
# Standard library
import os
import sys
import textwrap
import traceback

# Third-party
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


def get_tool_urls():
    LOGGER.info("Loading CC Legal Tool paths and adding prefix")
    file_path = shared.path_join(PATHS["data"], "legal-tool-paths.txt")
    prefix = "//creativecommons.org/"
    tool_urls = []
    with open(file_path, "r", encoding="utf-8") as file_obj:
        for line in file_obj:
            tool_urls.append(f"{prefix}{line.strip()}")
    return tool_urls


def sort_tools(path):
    dirs = path.strip().split("/")[3:]
    category = dirs[0]
    unit = dirs[1]
    if len(dirs) > 2:
        version = dirs[2]
        ver_sort = 999 - int(float(dirs[2]) * 10)
    else:
        version = None
        ver_sort = 999
    if len(dirs) == 4:
        jurisdiction = dirs[3]
        jur_sort = dirs[3]
    else:
        jurisdiction = None
        jur_sort = "A"
    # Priority 1: 4.0 licenses
    if category == "licenses" and version == "4.0":
        priority = 1
    # Priority 2: CC0 and PDM
    elif category == "publicdomain" and unit in ("mark", "zero"):
        priority = 2
    # Priority 3: unported 1.0-3.0 by* licenses
    elif (
        category == "licenses"
        and unit.startswith("by")
        and version is not None
        and jurisdiction is None
    ):
        priority = 3
    # Priority 4: ported 1.0-3.0 by* licenses
    elif (
        category == "licenses"
        and unit.startswith("by")
        and version is not None
        and jurisdiction is not None
    ):
        priority = 4
    # Priority 5: unported 1.0-3.0 non-by* licenses
    elif (
        category == "licenses"
        and not unit.startswith("by")
        and version is not None
        and jurisdiction is None
    ):
        priority = 5
    # Priority 6: ported 1.0-3.0 non-by* licenses
    elif (
        category == "licenses"
        and not unit.startswith("by")
        and version is not None
        and jurisdiction is not None
    ):
        priority = 6
    # Priority 7: miscellaneous
    else:
        priority = 7
    return f"{priority}-{ver_sort}-{jur_sort}-{unit}"


def save_tools_list(tool_urls):
    LOGGER.info("Saving prioritized CC Legal Tool URLs")
    file_path = shared.path_join(PATHS["data"], "prioritized-tool-urls.txt")
    tool_urls.append("")  # ensure file has end of file newline
    with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
        file_obj.writelines("\n".join(tool_urls))


def main():
    tool_urls = get_tool_urls()
    LOGGER.info("Prioritizing CC Legal Tool URLs")
    tool_urls.sort(key=sort_tools)
    save_tools_list(tool_urls)


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
