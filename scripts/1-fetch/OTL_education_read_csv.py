#!/usr/bin/env python
"""
Read from pre/automation OTL.csv file
"""

# Standard library
import argparse  # for parsing command line arguments
import csv  # for R/W csv viles

# library
import os  # for manipulating OS fs
import sys  # for using libraries
import textwrap  # for text formatting
import traceback  # for inspecting exceptions in the script

# Third-party
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# First-party/Local
import shared

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))  # noqa: E402


LOGGER, PATHS = shared.setup(__file__)

otl_csv_path = shared.path_join(
    PATHS["repo"], "pre-automation", "education", "datasets", "OTL.csv"
)
raw_data_file = shared.path_join(
    PATHS["data_quarter"], "1-fetch", "otl_raw_data.csv"
)
quater = os.path.basename(PATHS["data_quarter"])


LOGGER.info("Beginning execution of script.")


def argument_parser():
    """
    function to parse command line arguments and return the arguments parsed
    """
    LOGGER.info("options for argument parsing in command line")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable result saving",
    )
    return parser.parse_args()


def read_OTL_csv(args):
    """
    function to read the OTL csv file and save it to the data phase directory
    """
    LOGGER.info("Reading the OTL csv data file")

    if not os.path.exists(otl_csv_path):
        LOGGER.error(f"OTL csv file not found at {otl_csv_path}")
        raise shared.QuantifyingException(
            f"OTL.csv not found at {otl_csv_path}"
        )

    if not args.enable_save:
        LOGGER.info("Save disabled, skipping save step")
        return

    # creating the directory for the raw data if it does not exist
    os.makedirs(os.path.dirname(raw_data_file), exist_ok=True)

    # copying the csv file into the data directory
    with open(otl_csv_path, "r") as source:
        with open(raw_data_file, "w", newline="") as dest:
            reader = csv.reader(source)
            writer = csv.writer(dest)
            for row in reader:
                writer.writerow(row)

    LOGGER.info(f"Copied OTL.csv file to {raw_data_file}")


def main():
    args = argument_parser()
    shared.paths_log(LOGGER, PATHS)

    read_OTL_csv(args)


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
