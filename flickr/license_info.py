import flickrapi
import json


api_key = u'69f154651c83944ef0b07b91c8b87706'
api_secret = u'df4ab2a4358bc456'

flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')
licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))
photosJson = flickr.photos.search(license = 1)
photos = json.loads(photosJson.decode('utf-8'))
print(photos)

"""for i in [1, 2, 3, 4, 5, 6, 9, 10]:
    photosJson = flickr.photos.search(license = i)
    photos = json.loads(photosJson.decode('utf-8'))"""