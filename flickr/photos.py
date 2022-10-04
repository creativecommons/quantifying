import flickrapi
import json
import secrets

flickr = flickrapi.FlickrAPI(secrets.api_key, secrets.api_secret, format='json')

licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))

dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
for i in dic.keys():
    photosJson = flickr.photos.search(license=i, per_page=500)
    photos = json.loads(photosJson.decode('utf-8'))
with open('photos.json') as json_file:
    json.dump(dic, json_file)
