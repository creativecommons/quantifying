#!/usr/bin/env python
"""
Add project references.
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
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
SECTION = "Notes"


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


def data_locations(args):
    """
    Write References
    """
    shared.update_readme(
        args,
        SECTION,
        "Data locations",
        None,
        None,
        "This report was generated as part of:\n"
        "\n"
        "**[creativecommons/quantifying][repo]:** *quantify the size and"
        " diversity of the commons--the collection of works that are openly"
        " licensed or in the public domain*\n"
        "\nThe data used to generate this report is available in that"
        " repository at the following locations:\n"
        "\n"
        " | Resource        | Location |\n"
        " | --------------- | -------- |\n"
        " | Fetched data:   | [`../1-fetch/`](../1-fetch) |\n"
        " | Processed data: | [`../2-process/`](../2-process) |\n"
        " | Report data:    | [`../2-report/`](../2-report) |\n"
        "\n"
        "[repo]: https://github.com/creativecommons/quantifying\n",
    )


def usage(args):
    """
    Write copyright
    """
    shared.update_readme(
        args,
        SECTION,
        "Usage",
        None,
        None,
        "The Creative Commons (CC) icons, images, and logos are for use under"
        " the Creative Commons Trademark Policy (see [Policies - Creative"
        " Commons][ccpolicies]). **They *aren't* licensed under a Creative"
        " Commons license** (also see [Could I use a CC license to share my"
        " logo or trademark? - Frequently Asked Questions - Creative"
        " Commons][tmfaq]).\n"
        "\n"
        "[![CC0 1.0 Universal (CC0 1.0) Public Domain Dedication"
        "button][cc-zero-png]][cc-zero]\n"
        "Otherwise, this report is dedicated to the public domain under the"
        " [CC0 1.0 Universal (CC0 1.0) Public Domain Dedication][cc-zero].\n"
        "\n"
        "[ccpolicies]: https://creativecommons.org/policies\n"
        "[tmfaq]: https://creativecommons.org/faq/"
        "#could-i-use-a-cc-license-to-share-my-logo-or-trademark\n"
        "[cc-zero-png]: https://licensebuttons.net/l/zero/1.0/88x31.png"
        ' "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication button"\n'
        "[cc-zero]: https://creativecommons.org/publicdomain/zero/1.0/"
        ' "Creative Commons — CC0 1.0 Universal"',
    )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    data_locations(args)
    usage(args)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit References for {QUARTER}",
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
