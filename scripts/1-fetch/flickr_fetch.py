#!/usr/bin/env python
"""
Fetch CC photo license data from Flickr API for quarterly analysis.
"""

# Standard library
import argparse
import csv
import json
import os
import sys
import time
import traceback

# Third-party
import flickrapi
from dotenv import load_dotenv
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# First-party/Local
import shared  # noqa: E402

LOGGER, PATHS = shared.setup(__file__)
load_dotenv(PATHS["dotenv"])

FLICKR_API_KEY = os.getenv("FLICKR_API_KEY")
FLICKR_API_SECRET = os.getenv("FLICKR_API_SECRET")
FILE1_COUNT = os.path.join(PATHS["data_phase"], "flickr_1_count.csv")
HEADER1_COUNT = ["LICENSE_ID", "LICENSE_NAME", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

# flickr.photos.licenses.getInfo API
FLICKR_LICENSES = {
    1: "CC BY-NC-SA 2.0",
    2: "CC BY-NC 2.0",
    3: "CC BY-NC-ND 2.0",
    4: "CC BY 2.0",
    5: "CC BY-SA 2.0",
    6: "CC BY-ND 2.0",
    9: "Public Domain Dedication (CC0)",
    10: "Public Domain Mark",
}

CC_LICENSES = [1, 2, 3, 4, 5, 6, 9, 10]

LOGGER.info("Script execution started.")


def parse_arguments():
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Limit number of photos per license (default: 100)",
    )
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
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: generate fake data without API calls",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_flickr_api():
    LOGGER.info("Setting up Flickr API")

    if not FLICKR_API_KEY or not FLICKR_API_SECRET:
        raise shared.QuantifyingException(
            "Missing Flickr API credentials. Check your .env file."
        )

    return flickrapi.FlickrAPI(
        FLICKR_API_KEY,
        FLICKR_API_SECRET,
        format="json",
    )


def fetch_license_count(flickr, license_id, limit=100):
    """Fetch photo count for a specific license from Flickr API."""
    license_name = FLICKR_LICENSES.get(license_id, "Unknown")
    LOGGER.info(f"Fetching count for license {license_id}: {license_name}")

    try:
        photos_json = flickr.photos.search(
            license=license_id, per_page=min(limit, 500), page=1
        )

        photos_data = json.loads(photos_json.decode("utf-8"))

        if "photos" in photos_data and "total" in photos_data["photos"]:
            total = int(photos_data["photos"]["total"])
            count = min(total, limit)
            LOGGER.info(f"  Found {count} photos (total available: {total})")
            return count
        else:
            LOGGER.warning(f"  No data returned for license {license_id}")
            return 0

    except Exception as e:
        LOGGER.error(f"  Failed to fetch count for license {license_id}: {e}")
        return 0


def generate_fake_data(args):
    """Generate fake data for dev mode."""
    LOGGER.info("Creating fake data for dev mode")

    counts = {}
    base = args.limit // len(CC_LICENSES)
    for idx, license_id in enumerate(CC_LICENSES):
        counts[license_id] = base + (license_id * 10) + (idx * 5)

    return counts


def save_data(args, license_counts):
    """Save license count data to CSV file."""
    if not args.enable_save:
        LOGGER.info("Save disabled, skipping file write")
        return

    LOGGER.info(f"Writing data to {FILE1_COUNT}")

    data_rows = []
    for license_id, count in license_counts.items():
        data_rows.append(
            {
                "LICENSE_ID": license_id,
                "LICENSE_NAME": FLICKR_LICENSES[license_id],
                "COUNT": count,
            }
        )

    data_rows.sort(key=lambda x: x["LICENSE_ID"])

    with open(FILE1_COUNT, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADER1_COUNT)
        writer.writeheader()
        writer.writerows(data_rows)

    LOGGER.info(f"Successfully wrote {len(data_rows)} records")


def main():
    try:
        args = parse_arguments()

        if args.enable_git:
            shared.git_fetch_and_merge(args, PATHS["repo"])

        license_counts = {}

        if args.dev:
            license_counts = generate_fake_data(args)
        else:
            flickr = get_flickr_api()

            for license_id in CC_LICENSES:
                count = fetch_license_count(flickr, license_id, args.limit)
                license_counts[license_id] = count
                time.sleep(0.1)

        save_data(args, license_counts)

        if args.enable_git:
            args = shared.git_add_and_commit(
                args,
                PATHS["repo"],
                PATHS["data_quarter"],
                f"Add Flickr data for {QUARTER}",
            )
            shared.git_push_changes(args, PATHS["repo"])

        total_photos = sum(license_counts.values())
        LOGGER.info(f"Done. Total photos across all licenses: {total_photos}")

        for license_id in sorted(license_counts.keys()):
            count = license_counts[license_id]
            license_name = FLICKR_LICENSES[license_id]
            LOGGER.info(
                f"  License {license_id} ({license_name}): {count} photos"
            )

    except shared.QuantifyingException as e:
        LOGGER.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        LOGGER.error(f"Unexpected error: {e}")
        if LOGGER.isEnabledFor(10):
            traceback_str = traceback.format_exc()
            highlighted_traceback = highlight(
                traceback_str, PythonTracebackLexer(), TerminalFormatter()
            )
            print(highlighted_traceback)
        sys.exit(1)


if __name__ == "__main__":
    main()
