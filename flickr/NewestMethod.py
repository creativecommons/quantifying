"""
The following is a version of api call
optimized with taking up less memory
and no duplicate data (ideally)
step1: API call
step2: save useful data in the format of [[], []]
step3: saving lists of data to DataFrame
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

def to_df(datalis, namelis):
    df = [pd.DataFrame() for ind in range(len(datalis))]
    df = pd.DataFrame(datalis).transpose()
    df.columns = namelis
    # for count in range(len(temp_lis)):
    #     df[count] = df[count].append({name_lis[j]: temp_lis[j]}, ignore_index=True)
    return df


"""
this is to create one lindked list [[],[],[]] to save 
all the collumns with each collumn as a list
and to create one list to save all the nams for collumns
"""

def creat_lis(size):
    name_lis = [] * size
    return name_lis


def creat_lisoflis(size):
    temp_lis = [[] for i in range(size)]
    return temp_lis


retries = 0
flickr = flickrapi.FlickrAPI(secret.api_key, secret.api_secret, format='json')
license_lis = [1, 2, 3, 4, 5, 6, 9, 10]  # this is the cc licenses list

"""
we want to have these 11 columns of data saved in final csv
name_lis is the header of final table
temp_lis is in the form of list within list, which saves the actual data
each internal list is a column: ie. temp_lis[0] saves the data of id number
"""
name_lis = ["id", "dateuploaded", "isfavorite", "license", "realname",
            "location", "title", "description", "dates", "views", "comments", "tags"]
temp_lis = creat_lisoflis(len(name_lis))

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
                photosJson = flickr.photos.search(license=i, per_page=100, page=j)
                time.sleep(1)
                photos = json.loads(photosJson.decode('utf-8'))
                id = [x["id"] for x in photos["photos"]["photo"]]

                """
                change total everytime move to the 1st page of a new license
                and set the final CSV as empty every time start from the 1st page
                """
                if j == 1:
                    total = photos["photos"]["pages"]
                    data = pd.read_csv('final.csv')
                    for count in range(len(name_lis)):
                        data.drop(name_lis[count], inplace=True, axis=1)
                    data.to_csv("final.csv")

                '''
                use getInfo method to get more detailed photo info from inputting photo id
                and query data and save into linkedlist as columns of final dataset
                '''
                for index in range(0, len(id)):
                    detailJson = flickr.photos.getInfo(license=i, photo_id=id[index])
                    time.sleep(1)
                    photos_detail = json.loads(detailJson.decode('utf-8'))
                    print(index, "id out of", len(id), "in license", i, "page", j, "out of", total)

                    """below is the query process of useful data"""
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
                            """
                            some photo id has more than one subids included, each corresponds to certain tag(s)
                            therefore we save tags of each id as a list
                            further clean/query may be needed in analyzing this column of data
                            """
                            if photos_detail["photo"]["tags"]["tag"]:
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
                and merge history CSVs to final CSV
                and update j(the current page number in txt)
                note that the map(pd.read_csv) means overwrite data each time so duplicate issue solved
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
                temp_lis = creat_lisoflis(len(name_lis))

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
                    temp_lis = creat_lisoflis(len(name_lis))  # clear list everytime before rerun (prevent duplicate)
                    break

    except Exception as e:
        retries += 1
        print(e)
        print("page", j, "out of", total, "in license", i, "with retry number", retries)
        temp_lis = creat_lisoflis(len(name_lis))  # clear list everytime before rerun (prevent duplicate)
        continue