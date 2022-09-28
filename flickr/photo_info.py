import flickrapi
import json
import secrets

with open('photos.json') as json_file:
    photo = json.load(json_file)


flickr = flickrapi.FlickrAPI(secrets.api_key, secrets.api_secret, format='json')

# be careful to run the below api call (3600 limits/hr)
# dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
# keys = [1, 2, 3, 4, 5, 6, 9, 10]
# info_dic = {}
# for j in keys:
#     for k in range(0, len((photo[str(j)]["photos"]["photo"]).keys())):
#         info_dic[photo[str(j)]["photos"]["photo"][k]["id"]] = flickr.photos.getInfo(photo[str(j)]["photos"]["photo"][k]["id"])

print(len(photo[str(1)]["photos"]["photo"]))
print(len(photo[str(2)]["photos"]["photo"]))
print(len(photo[str(3)]["photos"]["photo"]))
print(len(photo[str(4)]["photos"]["photo"]))
print(len(photo[str(5)]["photos"]["photo"]))
print(len(photo[str(6)]["photos"]["photo"]))
print(len(photo[str(9)]["photos"]["photo"]))
print(len(photo[str(10)]["photos"]["photo"]))
#print(photo[str(1)]["photos"]["photo"][1]["id"])
