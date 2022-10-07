import flickrapi
import json
import secrets
import time

# Build API call and use the getInfo flickr built-in method to get
# info about which license is CC license
flickr = flickrapi.FlickrAPI(secrets.api_key, secrets.api_secret, format='json')

licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))

# CC licenses are 1, 2, 3, 4, 5, 6, 9, 10
# create a dictionary dic2 to record the licenses and photos included in each license
dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
dic2 = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 9: [], 10: []}

try:
    total = 3  # total means total number of pages for each license
    # use record_j txt to record j every iterating each page of photos
    # to pick up from where the script errors or stops
    with open('record_j.txt') as f:
        j = int(f.read())
    for i in dic2.keys():
        while j < total:
            # use search method to pull photo id included under each license
            photosJson = flickr.photos.search(license=i, per_page=500, page=j, min_upload_date = '2010-01-01 00:00:00', max_upload_date = '2022-10-05 00:00:00')
            time.sleep(1)
            photos = json.loads(photosJson.decode('utf-8'))
            id = [x["id"] for x in photos["photos"]["photo"]]
            if j == 1:  # change total everytime move to the 1st page of a license
                total = photos["photos"]["pages"]

            # use getInfo method to get more detailed photo info from inputting photo id
            # load json data into photos_detail.json
            for index in range(0, len(id)):
                detailJson = flickr.photos.getInfo(license=i, photo_id=id[index])
                time.sleep(1)
                photos_detail = json.loads(detailJson.decode('utf-8'))
                print(index, "id out of", len(id), "in license", i, "page", j, "out of", total)
                dic2[i].append(photos_detail)
            j += 1
            print(j, "out of", total, "in license", i)
            if j > 1 and dic2[i][0] == photos:
                break
            with open('photos_detail.json', 'w') as json_file:  # save data once after iterating each page
                json.dump(dic2, json_file)
            with open('record_j.txt', 'w') as f:
                f.write(str(j))
except Exception as e:
    with open('photos_detail.json', 'w') as json_file:
        json.dump(dic2, json_file)
    print(e)
with open('photos_detail.json', 'w') as json_file:
    json.dump(dic2, json_file)

