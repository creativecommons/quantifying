import flickrapi
import json
import secrets
import time


flickr = flickrapi.FlickrAPI(secrets.api_key, secrets.api_secret, format='json')

licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))

dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
dic2 = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 9: [], 10: []}

try:
    with open('record_j.txt') as f:
        j = int(f.read())
    for i in dic2.keys():
        total = 3
        while j < total:
            photosJson = flickr.photos.search(license=i, per_page=500, page=j)
            photos = json.loads(photosJson.decode('utf-8'))
            time.sleep(1)
            id = [x["id"] for x in photos["photos"]["photo"]]
            if j == 1:  # change total everytime move to the 1st page of a license
                total = min(photos["photos"]["pages"], 110000)
            for index in range(0, len(id)):
                detailJson = flickr.photos.getInfo(license=i, photo_id=id[index])
                photos_detail = json.loads(detailJson.decode('utf-8'))
                print(index, "id out of", len(id), "in license", i, "page", j, "out of", total)
                time.sleep(1)
                dic2[i].append(photos_detail)
            j += 1
            print(j, "out of", total, "in license", i)
            if j > 1 and dic2[i][0] == photos:
                break
            with open('photos_detail.json', 'w') as json_file:  # save once after iterating each page
                json.dump(dic2, json_file)
            with open('record_j.txt', 'w') as f:
                f.write(str(j))
except Exception as e:
    with open('photos_detail.json', 'w') as json_file:
        json.dump(dic2, json_file)
    print(e)
with open('photos_detail.json', 'w') as json_file:
    json.dump(dic2, json_file)

