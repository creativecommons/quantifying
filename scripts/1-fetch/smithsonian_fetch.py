#!/usr/bin/env python
"""
Fetch metrics usage from Smithsonian Institution Open Access API.
"""

# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback
from operator import itemgetter

# Third-party
import requests
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
DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
FILE_1_METRICS = os.path.join(PATHS["data_phase"], "smithsonian_1_metrics.csv")
FILE_2_UNITS = os.path.join(PATHS["data_phase"], "smithsonian_2_units.csv")
HEADER_1_METRICS = [
    "CC0_RECORDS",
    "CC0_RECORDS_WITH_CC0_MEDIA",
    "CC0_MEDIA",
    "CC0_MEDIA_PERCENTAGE",
    "TOTAL_OBJECTS",
]
HEADER_2_UNITS = [
    "UNIT",
    "CC0_RECORDS",
    "CC0_RECORDS_WITH_CC0_MEDIA",
    "TOTAL_OBJECTS",
]
QUARTER = os.path.basename(PATHS["data_quarter"])

unit_map = {
    "AAA": "Archives of American Art",
    "AAG": "Archives of American Gardens",
    "ACM": "Anacostia Community Museum",
    "ACMA": "Anacostia Community Museum Archives",
    "CFCHFOLKLIFE": "Ralph Rinzler Folklife Archives and Collections",
    "CHNDM": "Cooper Hewitt, Smithsonian Design Museum",
    "FBR": "Smithsonian Field Book Project",
    "FSG": "Freer Gallery of Art and Arthur M. Sackler Gallery",
    "HAC": "Smithsonian Gardens",
    "HMSG": "Hirshhorn Museum and Sculpture Garden",
    "HSFA": "Human Studies Film Archives",
    "NASM": "National Air and Space Museum",
    "NMAAHC": "National Museum of African American History and Culture",
    "NMAH": "National Museum of American History",
    "NMAI": "National Museum of the American Indian",
    "NMAfA": "National Museum of African Art",
    "NMNHANTHRO": "NMNH - Anthropology Dept.",
    "NMNHBIRDS": "NMNH - Vertebrate Zoology - Birds Division",
    "NMNHBOTANY": "NMNH - Botany Dept.",
    "NMNHEDUCATION": "NMNH - Education & Outreach",
    "NMNHENTO": "NMNH - Entomology Dept.",
    "NMNHFISHES": "NMNH - Vertebrate Zoology - Fishes Division",
    "NMNHHERPS": "NMNH - Vertebrate Zoology - Herpetology Division",
    "NMNHINV": "NMNH - Invertebrate Zoology Dept.",
    "NMNHMAMMALS": "NMNH - Vertebrate Zoology - Mammals Division",
    "NMNHMINSCI": "NMNH - Mineral Sciences Dept.",
    "NMNHPALEO": "NMNH - Paleobiology Dept.",
    "NPG": "National Portrait Gallery",
    "NPM": "National Postal Museum",
    "NZP": "Smithsonian's National Zoo & Conservation Biology Institute",
    "OCIO_DPO3D": "OCIO Digital Preservation & 3D Team",
    "OFEO-SG": "Office of Facilities Engineering &"
    " Operations – Smithsonian Gardens",
    "SAAM": "Smithsonian American Art Museum",
    "SIA": "Smithsonian Institution Archives",
    "SIL": "Smithsonian Libraries",
    "SILAF": "Smithsonian Institution Libraries, African Section",
    "SILNMAHTL": "Smithsonian Institution Libraries,"
    " National Museum of American History, Library",
    "SLA_SRO": "Smithsonian Libraries Archives, Special Research/Operations",
}


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, and push)",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def check_for_completion():
    completed_metrics = False
    completed_units = False

    try:
        with open(FILE_1_METRICS, "r", newline="") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            if len(list(reader)) > 0:
                completed_metrics = True
    except FileNotFoundError:
        pass  # File may not be found without --enable-save, etc.

    try:
        with open(FILE_2_UNITS, "r", newline="") as file_obj:
            reader = csv.DictReader(file_obj, dialect="unix")
            if len(list(reader)) > 30:
                completed_units = True
    except FileNotFoundError:
        pass  # File may not be found without --enable-save, etc.

    if completed_metrics and completed_units:
        raise shared.QuantifyingException(
            f"Data fetch completed for {QUARTER}", 0
        )


def write_data(args, data_metrics, data_units):
    if not args.enable_save:
        return args

    # Create data directory for this phase
    os.makedirs(PATHS["data_phase"], exist_ok=True)

    with open(FILE_1_METRICS, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_1_METRICS, dialect="unix"
        )
        writer.writeheader()
        for row in data_metrics:
            writer.writerow(row)

    with open(FILE_2_UNITS, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.DictWriter(
            file_obj, fieldnames=HEADER_2_UNITS, dialect="unix"
        )
        writer.writeheader()
        for row in data_units:
            writer.writerow(row)

    return args


def fetch_unit_codes(session):
    LOGGER.info("Fetching current unit codes from Smithsonian API")
    url = "https://api.si.edu/openaccess/api/v1.0/terms/unit_code"
    params = {"api_key": DATA_GOV_API_KEY}
    try:
        with session.get(url, params=params) as response:
            response.raise_for_status()
            api_codes = set(response.json()["response"]["terms"])
    except requests.HTTPError as e:
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        raise shared.QuantifyingException(f"KeyError: {e}", 1)

    map_codes = set(unit_map.keys())
    new_codes = sorted(api_codes - map_codes)
    removed_codes = sorted(map_codes - api_codes)

    if new_codes:
        LOGGER.warning(f"New unit code(s) not in unit_map: {new_codes}")
    if removed_codes:
        LOGGER.warning(f"unit_map code(s) no longer in API: {removed_codes}")
    if not new_codes and not removed_codes:
        LOGGER.info("unit_map is up to date")


def query_smithsonian(args, session):
    if not DATA_GOV_API_KEY:
        raise shared.QuantifyingException(
            "Authentication (DATA_GOV_API_KEY) required. Please ensure your"
            " API key is set in .env",
            1,
        )
    LOGGER.info("Fetch CC0 metrics and units from units from Smithsonain")
    url = "https://api.si.edu/openaccess/api/v1.0/stats"
    params = {"api_key": DATA_GOV_API_KEY}
    try:
        with session.get(url, params=params) as response:
            response.raise_for_status()
            data = response.json()["response"]
    except requests.HTTPError as e:
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)
    except KeyError as e:
        raise shared.QuantifyingException(f"KeyError: {e}", 1)
    data_metrics = [
        {
            "CC0_MEDIA": data["metrics"]["CC0_media"],
            "CC0_MEDIA_PERCENTAGE": data["metrics"]["CC0_media_percentage"],
            "CC0_RECORDS": data["metrics"]["CC0_records"],
            "CC0_RECORDS_WITH_CC0_MEDIA": data["metrics"][
                "CC0_records_with_CC0_media"
            ],
            "TOTAL_OBJECTS": data["total_objects"],
        }
    ]
    data_units = []
    for unit in data["units"]:
        if unit["total_objects"] == 0:
            continue
        data_units.append(
            {
                "UNIT": unit_map.get(unit["unit"], unit["unit"]),
                "CC0_RECORDS": unit["metrics"]["CC0_records"],
                "CC0_RECORDS_WITH_CC0_MEDIA": unit["metrics"][
                    "CC0_records_with_CC0_media"
                ],
                "TOTAL_OBJECTS": unit["total_objects"],
            }
        )
    data_units = sorted(data_units, key=itemgetter("UNIT"))
    LOGGER.info(f"Fetched stats for {len(data_units)} units")
    return data_metrics, data_units


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    check_for_completion()
    session = shared.get_session()
    fetch_unit_codes(session)
    data_metrics, data_units = query_smithsonian(args, session)
    args = write_data(args, data_metrics, data_units)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new Smithsonian data for {QUARTER}",
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
