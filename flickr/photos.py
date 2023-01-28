# Standard library
import json
import os.path
import sys
import traceback

# Third-party
import flickrapi
import query_secrets

CWD = os.path.dirname(os.path.abspath(__file__))


def main():
    flickr = flickrapi.FlickrAPI(
        query_secrets.api_key, query_secrets.api_secret, format="json"
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
