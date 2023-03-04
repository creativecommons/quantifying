# Standard library
import json
import os
import os.path
import sys
import traceback

# Third-party
import flickrapi
from dotenv import load_dotenv

CWD = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(os.path.dirname(CWD), ".env")
load_dotenv(dotenv_path)


def main():
    flickr = flickrapi.FlickrAPI(
        os.getenv("API_KEY"), os.getenv("API_SECRET"), format="json"
    )

    # use search method to pull general photo info under each cc license data
    # saved in photos.json
    dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
    for i in dic.keys():
        photosJson = flickr.photos.search(license=i, per_page=500)
        dic[i] = [json.loads(photosJson.decode("utf-8"))]
    with open(os.path.join(CWD, "photos.json"), "w") as json_file:
        json.dump(dic, json_file)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        print("INFO (130) Halted via KeyboardInterrupt.", file=sys.stderr)
        sys.exit(130)
    except Exception:
        print("ERROR (1) Unhandled exception:", file=sys.stderr)
        print(traceback.print_exc(), file=sys.stderr)
    sys.exit(1)
