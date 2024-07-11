"""
Fetching photo information from Flickr API for photos under
each Creative Commons license and saving the data into CSV files.
"""

# Standard library
# import json
import os
import sys
import traceback

# Third-party
import flickrapi
import pandas as pd
from dotenv import load_dotenv

# from datetime import datetime


sys.path.append(".")
# First-party/Local
import shared  # noqa: E402

# Setup paths and LOGGER using quantify.setup()
_, PATH_WORK_DIR, PATH_DOTENV, _, LOGGER = shared.setup(__file__)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Log the start of the script execution
LOGGER.info("Script execution started.")

# Define Flickr API key and secret
FLICKR_API_KEY = os.getenv("FLICKR_API_KEY")
FLICKR_API_SECRET = os.getenv("FLICKR_API_SECRET")

# Initialize Flickr API instance
flickr = flickrapi.FlickrAPI(
    FLICKR_API_KEY, FLICKR_API_SECRET, format="parsed-json"
)

# List of Creative Commons licenses
licenses = {
    "1": "Attribution-NonCommercial-ShareAlike License",
    "2": "Attribution-NonCommercial License",
    "3": "Attribution-NonCommercial-NoDerivs License",
    "4": "Attribution License",
    "5": "Attribution-ShareAlike License",
    "6": "Attribution-NoDerivs License",
    "7": "No known copyright restrictions",
    "8": "United States Government Work",
    "9": "Public Domain Dedication (CC0)",
    "10": "Public Domain Mark",
}


def fetch_photos(license_id, page=1):
    """
    Fetch photos for a specific license from the Flickr API.
    Args:
    - license_id (int): License ID.
    - page (int): Page number for pagination.
    Returns:
    - List of photo data.
    """
    try:
        photos = flickr.photos.search(
            license=license_id,
            per_page=100,
            page=page,
            extras="date_taken,license,description,tags,views,comments,geo",
        )
        return photos["photos"]["photo"]
    except Exception as e:
        LOGGER.error(
            f"Error fetching photos for license {license_id} "
            f"on page {page}: {e}"
        )
        return []


def process_photo_data(photos):
    """
    Process photo data and extract the required fields.
    Args:
    - photos (list): List of photo data.
    Returns:
    - List of dictionaries with the processed data.
    """
    processed_data = []
    for photo in photos:
        location = f"{photo.get('latitude', '')}, {photo.get('longitude', '')}"
        dates = photo.get("datetaken", "")
        license_id = photo.get("license", "")
        description = photo.get("description", {}).get("_content", "")
        tags = photo.get("tags", "").split()
        views = photo.get("views", 0)
        comments = photo.get("comments", 0)
        processed_data.append(
            {
                "location": location,
                "dates": dates,
                "license": license_id,
                "description": description,
                "tags": tags,
                "views": views,
                "comments": comments,
            }
        )
    return processed_data


def save_to_csv(data, license_name):
    """
    Save processed data to a CSV file.
    Args:
    - data (list): List of dictionaries with processed data.
    - license_name (str): Name of the license.
    """
    df = pd.DataFrame(data)
    csv_file = os.path.join(
        PATH_WORK_DIR, f"{license_name.replace(' ', '_')}.csv"
    )
    df.to_csv(csv_file, index=False)
    LOGGER.info(f"CSV file generated for license: {license_name}")


def main():
    for license_id, license_name in licenses.items():
        page = 1
        all_photos = []
        while True:
            photos = fetch_photos(license_id, page)
            if not photos:
                break
            processed_data = process_photo_data(photos)
            all_photos.extend(processed_data)
            page += 1
        if all_photos:
            save_to_csv(all_photos, license_name)
        else:
            LOGGER.info(f"No photos found for license: {license_name}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
