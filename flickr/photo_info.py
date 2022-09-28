import flickrapi
import json

with open('photos.json') as json_file:
    photo = json.load(json_file)

api_key = u'69f154651c83944ef0b07b91c8b87706'
api_secret = u'df4ab2a4358bc456'
flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')

# be careful to run the below api call (3600 limits/hr)
dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
keys = [1, 2, 3, 4, 5, 6, 9, 10]
info_dic = {}
for j in keys:
    for k in range(0, len((photo[str(j)]["photos"]["photo"]).keys())):
        info_dic[photo[str(j)]["photos"]["photo"][k]["id"]] = flickr.photos.getInfo(photo[str(j)]["photos"]["photo"][k]["id"])

#print(len(photo[str(1)]["photos"]["photo"]))
#print(photo[str(1)]["photos"]["photo"][1]["id"])
