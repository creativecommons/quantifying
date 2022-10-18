"""
CURRENT BUG: to_df func doesn't work and df has the type of
list instead of dataframe
The following is a version of api call that still have bugs
with saving json to dataframe step included
and probably won't take up much too memories
"""

import flickrapi
import json
import secret
import time
import pyautogui
import random
import pandas as pd

"""two functions of querying data"""


def data_query(raw, part, detail, temp_lis, index):  # part and detail should be string
    query = raw["photo"][part][detail]
    temp_lis[index].append(query)
    return temp_lis


def query(raw, part, temp_lis, index):  # part should be string
    query = raw["photo"][part]
    temp_lis[index].append(query)
    return temp_lis


"""
this is to transform pulled and queried data into dataframe
by iterating through the list of columns
"""


def to_df(linkedlis, namelis):
    df = [pd.DataFrame() for ind in range(len(linkedlis))]
    for count in range(len(linkedlis)):
        df[count] = df[count].append({namelis[j]: linkedlis[j]}, ignore_index=True)
    return df


"""
this is to create one lindked list [[],[],[]] to save 
all the collumns with each collumn as a list
and to create one list to save all the nams for collumns
"""


def creat_lis(size):
    name_lis = [] * size
    return name_lis


def creat_linkedlis(size):
    temp_lis = [[] for i in range(size)]
    return temp_lis


retries = 0
flickr = flickrapi.FlickrAPI(secret.api_key, secret.api_secret, format='json')
license_lis = [1, 2, 3, 4, 5, 6, 9, 10]  # this is the cc licenses list
name_lis = ["id", "dateuploaded", "isfavorite", "license", "realname",
            "location", "title", "description", "dates", "views", "comments", "tags"]
temp_lis = creat_linkedlis(len(name_lis))

while True:
    try:
        '''
        use rec txt to record j(current page), i(current license), and total
        every time iterating through one page of photos
        to pick up from where the script errors or stops
        '''
        with open('rec.txt') as f:
            readed = f.read().split(" ")
            j = int(readed[0])
            i = int(readed[1])
            total = int(readed[2])
        while i in license_lis:
            while j <= total:
                # use search method to pull photo id included under each license
                photosJson = flickr.photos.search(license=i, per_page=10, page=j)
                time.sleep(1)
                photos = json.loads(photosJson.decode('utf-8'))
                id = [x["id"] for x in photos["photos"]["photo"]]

                """change total everytime move to the 1st page of a new license"""
                if j == 1:
                    total = photos["photos"]["pages"]

                '''
                use getInfo method to get more detailed photo info from inputting photo id
                and query data and save into linkedlist as columns of final dataset
                '''
                for index in range(0, len(id)):
                    detailJson = flickr.photos.getInfo(license=i, photo_id=id[index])
                    time.sleep(1)
                    photos_detail = json.loads(detailJson.decode('utf-8'))
                    print(index, "id out of", len(id), "in license", i, "page", j, "out of", total)
                    for a in range(0, len(name_lis)):
                        # name_lis = ["id", "dateuploaded", "isfavorite", "license", "realname",
                        #  "location", "title", "description", "dates", "views", "comments", "tags"]
                        if (a >= 0 and a < 4) or a == 9:
                            temp_lis = query(photos_detail, name_lis[a], temp_lis, a)
                        if a == 4 or a == 5:
                            temp_lis = data_query(photos_detail, "owner", name_lis[a], temp_lis, a)
                        if a == 6 or a == 7 or a == 10:
                            temp_lis = data_query(photos_detail, name_lis[a], "_content", temp_lis, a)
                        if a == 8:
                            temp_lis = data_query(photos_detail, name_lis[a], "taken", temp_lis, a)
                        if a == 11:
                            if photos_detail["photo"]["tags"]["tag"]:
                                # print(photos_detail["photo"]["tags"]["tag"][0])
                                temp_lis[a].append([photos_detail["photo"]["tags"]["tag"][num]["raw"]
                                                    for num in range(len(photos_detail["photo"]["tags"]["tag"]))])
                            else:
                                temp_lis = data_query(photos_detail, name_lis[a], "tag", temp_lis, a)
                    if index % 100 == 0:
                        # prevent laptop from sleeping
                        pyautogui.moveTo(random.randint(50, 300), random.randint(50, 300))

                j += 1
                print("page", j, "out of", total, "in license", i, "with retry number", retries)

                """
                now we will put the list of columns into dataframe
                and save the dataframe into history csv after iterating through each page
                and merge history csvs to final csv
                 and update (j) the current page number in txt
                """
                df = to_df(temp_lis, name_lis)
                # print(type(df))
                # print(name_lis, temp_lis, df)
                df.to_csv('hs.csv')
                df = pd.concat(
                    map(pd.read_csv, ['hs.csv', 'final.csv']), ignore_index=True)
                df.to_csv('final.csv')
                with open('rec.txt', 'w') as f:
                    f.write(str(j) + " " + str(i) + " " + str(total))

                """
                set list to empty everytime after saving the data into
                the csv file to prevent from saving duplicate data
                """
                temp_lis = creat_linkedlis(len(name_lis))

                '''
                if current page number has reached the max limit of total pages
                reset j to 1 and update i to the license in the dictionary
                '''
                if j > total:
                    i += 1
                    j = 1
                    while i not in license_lis:
                        i += 1
                    with open('rec.txt', 'w') as f:
                        f.write(str(j) + " " + str(i) + " " + str(total))
                    break

    except Exception as e:
        retries += 1
        print(e)
        print("page", j, "out of", total, "in license", i, "with retry number", retries)
        temp_lis = creat_linkedlis(len(name_lis))
        continue
