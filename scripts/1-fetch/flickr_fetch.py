#!/usr/bin/env python
"""
Fetch CC photo data from Flickr API for quarterly analysis.
"""
import argparse
import csv
import json
import os
import sys
import time
import traceback

import flickrapi
import pandas as pd
from dotenv import load_dotenv
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import shared  # noqa: E402

LOGGER, PATHS = shared.setup(__file__)
load_dotenv(PATHS["dotenv"])
FLICKR_API_KEY = os.getenv("FLICKR_API_KEY")
FLICKR_API_SECRET = os.getenv("FLICKR_API_SECRET")
FILE1_COUNT = os.path.join(PATHS["data_phase"], "flickr_1_count.csv")
HEADER1_COUNT = ["LICENSE_ID", "LICENSE_NAME", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])

FLICKR_LICENSES = {
    1: "All Rights Reserved",
    2: "Attribution-NonCommercial-ShareAlike License",
    3: "Attribution-NonCommercial License", 
    4: "Attribution-NonCommercial-NoDerivs License",
    5: "Attribution License",
    6: "Attribution-ShareAlike License",
    7: "Attribution-NoDerivs License",
    8: "No known copyright restrictions",
    9: "United States Government Work",
    10: "Public Domain Dedication (CC0)",
}

CC_LICENSES = [2, 3, 4, 5, 6, 10]

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
        help="Development mode: avoid hitting API (generate fake data)",
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
    license_name = FLICKR_LICENSES[license_id]
    LOGGER.info(f"Getting count for license {license_id}: {license_name}")
    
    try:
        photos_json = flickr.photos.search(
            license=license_id,
            per_page=min(limit, 500),
            page=1
        )
        
        photos_data = json.loads(photos_json.decode("utf-8"))
        
        if "photos" in photos_data and "total" in photos_data["photos"]:
            total = int(photos_data["photos"]["total"])
            return min(total, limit)
        else:
            LOGGER.warning(f"No data for license {license_id}")
            return 0
            
    except Exception as e:
        LOGGER.error(f"Failed to get count for license {license_id}: {e}")
        return 0


def save_data(args, license_counts):
    if not args.enable_save:
        LOGGER.info("Save disabled, skipping")
        return
        
    LOGGER.info(f"Writing data to {FILE1_COUNT}")
    
    data_rows = []
    for license_id, count in license_counts.items():
        data_rows.append({
            "LICENSE_ID": license_id,
            "LICENSE_NAME": FLICKR_LICENSES[license_id],
            "COUNT": count
        })
    
    with open(FILE1_COUNT, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADER1_COUNT)
        writer.writeheader()
        writer.writerows(data_rows)
    
    LOGGER.info(f"Wrote {len(data_rows)} records")


def generate_fake_data(args):
    LOGGER.info("Creating fake data for dev mode")
    
    fake_counts = {}
    for license_id in CC_LICENSES:
        fake_counts[license_id] = args.limit // len(CC_LICENSES) + (license_id * 10)
    
    return fake_counts


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
        LOGGER.info(f"Done. Total photos: {total_photos}")
        
        for license_id, count in license_counts.items():
            LOGGER.info(f"License {license_id}: {count} photos")
            
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
