import flickrapi
import json


api_key = u'69f154651c83944ef0b07b91c8b87706'
api_secret = u'df4ab2a4358bc456'
flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')

licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))

dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
for i in dic.keys():
    photosJson = flickr.photos.search(license = i)
    photos = json.loads(photosJson.decode('utf-8'))
    dic[i] = photos

with open('photos.json', 'w') as json_file:
    json.dump(dic, json_file)

