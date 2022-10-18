"""
The following is a runnable version of api call
but hasn't had saving json to datafram step included
"""

import flickrapi
import json
import secret
import time

import pyautogui
import random

'''
Build API call and use the getInfo flickr built-in method to get
info about which license is CC license
'''
flickr = flickrapi.FlickrAPI(secret.api_key, secret.api_secret, format='json')

licenseJson = flickr.photos.licenses.getInfo()
licenseInfo = json.loads(licenseJson.decode('utf-8'))

'''
CC licenses are 1, 2, 3, 4, 5, 6, 9, 10
Here we create a dictionary dic2 to record 
the licenses and photos included in each license
'''
dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
dic2 = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 9: [], 10: []}
retries = 0
indexOfKey = 0

while True:
    try:
        '''
        use record_j txt to record j(current page), i(current license), and total
        every time iterating through one page of photos
        to pick up from where the script errors or stops
        '''
        with open('record.txt') as f:
            readed = f.read().split(" ")
            j = int(readed[0])
            i = int(readed[1])
            total = int(readed[2])
        while i in dic2.keys():
            while j <= total:
                # use search method to pull photo id included under each license
                photosJson = flickr.photos.search(license=i, per_page=500, page=j)
                time.sleep(1)
                photos = json.loads(photosJson.decode('utf-8'))
                id = [x["id"] for x in photos["photos"]["photo"]]
                if j == 1:  # change total everytime move to the 1st page of a new license
                    total = photos["photos"]["pages"]

                '''
                use getInfo method to get more detailed photo info from inputting photo id
                to load json data into photos_detail.json
                '''
                for index in range(0, len(id)):
                    detailJson = flickr.photos.getInfo(license=i, photo_id=id[index])
                    time.sleep(1)
                    photos_detail = json.loads(detailJson.decode('utf-8'))
                    print(index, "id out of", len(id), "in license", i, "page", j, "out of", total)
                    dic2[i].append(photos_detail)

                    if index % 100 == 0:
                        # prevent laptop from sleeping
                        pyautogui.moveTo(random.randint(50, 300), random.randint(50, 300))

                j += 1
                print("page", j, "out of", total, "in license", i, "with retry number", retries)

                '''
                append data to the last record once after iterating each page
                '''
                with open('photos_detail.json', 'a') as json_file:
                    json.dump(dic2, json_file)
                with open('record.txt', 'w') as f:
                    f.write(str(j) + " " + str(i) + " " + str(total))

                """
                set dictionary to original one everytime after saving the data into
                photos_detail json file to prevent from saving duplicate data
                """
                dic2 = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 9: [], 10: []}

                '''
                if current page number has reached the limit of total pages
                reset j to 1 and update i to the license in the dictionary
                '''
                if j > total:
                    temp = list(dic2)
                    i += 1
                    j = 1
                    while i not in dic2.keys():
                        i += 1
                    with open('record.txt', 'w') as f:
                        f.write(str(j) + " " + str(i) + " " + str(total))
                    break

    except Exception as e:
        retries += 1
        with open('photos_detail.json', 'a') as json_file:
            json.dump(dic2, json_file)
        print(e)
        print("page", j, "out of", total, "in license", i, "with retry number", retries)
        continue

with open('photos_detail.json', 'a') as json_file:
    json.dump(dic2, json_file)
