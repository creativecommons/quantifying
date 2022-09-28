import flickrapi
import json


api_key = u'69f154651c83944ef0b07b91c8b87706'
api_secret = u'df4ab2a4358bc456'
flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')

licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))

dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
dic2 = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 9: [], 10: []}

try:
    for i in dic2.keys():
        size = 0
        total = 2
        for j in range(1, total):
            photosJson = flickr.photos.search(license=i, per_page=500, page=j)
            photos = json.loads(photosJson.decode('utf-8'))
            if j == 1:
                total = photos["photos"]["pages"]
            size += 1
            print(size, "out of", total, "in license", i)
            if size > 1 and dic2[i][0] == photos:
                break
            dic2[i].append(photos)
except Exception as e:
    with open('photos.json', 'w') as json_file:
        json.dump(dic2, json_file)
    print(e)
with open('photos.json', 'w') as json_file:
    json.dump(dic2, json_file)


# for j in range(1, 3):
#     photos = flickr.photos.search(license=1, per_page=500, page=j)
#     dic2[1].append(photos)
#
# print(len(dic2[1]))



# with open('photos.json', 'w') as json_file:
#     json.dump(dic, json_file)

