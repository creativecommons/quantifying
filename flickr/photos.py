"""
Fetching photo information from Flickr API for photos under
each Creative Commons license and saving the data into a JSON file
"""

# Standard library
import json
import logging
import os
import sys

# Third-party
import flickrapi
from dotenv import load_dotenv

# First-party/Local
import quantify

# Setup paths, and LOGGER using quantify.setup()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
_, PATH_WORK_DIR, PATH_DOTENV, _, LOGGER = quantify.setup(__file__)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Set up the logger
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Define both the handler and the formatter
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# Add formatter to the handler
handler.setFormatter(formatter)

# Add handler to the logger
LOG.addHandler(handler)

# Log the start of the script execution
LOG.info("Script execution started.")


def main():
    # Initialize Flickr API instance
    flickr = flickrapi.FlickrAPI(
        os.getenv("FLICKR_API_KEY"),
        os.getenv("FLICKR_API_SECRET"),
        format="json",
    )

    # Dictionary to store photo data for each Creative Commons license
    dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
    # Use search method to retrieve photo info for each license
    # and store it in the dictionary
    for i in dic.keys():
        photosJson = flickr.photos.search(license=i, per_page=500)
        dic[i] = [json.loads(photosJson.decode("utf-8"))]
    # Save the dictionary containing photo data to a JSON file
    with open(os.path.join(PATH_WORK_DIR, "photos.json"), "w") as json_file:
        json.dump(dic, json_file)


if __name__ == "__main__":
    # Exception Handling
    try:
        main()
    except SystemExit as e:
        LOGGER.error("System exit with code: %d", e.code)
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception("Unhandled exception:")
        sys.exit(1)
