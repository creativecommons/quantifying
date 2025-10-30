"""
Fetching photo information from Flickr API for photos under
each Creative Commons license and saving the data into a JSON file
"""

# Standard library
import json
import os
import sys
import traceback

# Third-party
import flickrapi
from dotenv import load_dotenv

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Setup paths, and LOGGER using quantify.setup()
_, PATH_WORK_DIR, PATH_DOTENV, _, LOGGER = quantify.setup(__file__)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Log the start of the script execution
LOGGER.info("Script execution started.")


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
