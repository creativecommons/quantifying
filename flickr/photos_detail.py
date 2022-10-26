"""
The following is a version of api call
optimized with taking up less memory
and no duplicate data (ideally)
step1: API call
step2: save useful data in the format of [[], []]
step3: saving lists of data to DataFrame
"""
# Standard library
import json
import random
import time

# Third-party
import flickrapi
import pandas as pd
import pyautogui
import secret_key

"""two functions of querying data"""


def data_query(raw, part, detail, temp_list, index):
    # part and detail should be string
    query = raw["photo"][part][detail]
    temp_list[index].append(query)
    return temp_list


def query(raw, part, temp_list, index):  # part should be string
    query = raw["photo"][part]
    temp_list[index].append(query)
    return temp_list


"""
this is to transform pulled and queried data into dataframe
by iterating through the list of columns
"""


def to_df(datalist, namelist):
    df = [pd.DataFrame() for ind in range(len(datalist))]
    df = pd.DataFrame(datalist).transpose()
    df.columns = namelist
    return df


"""
this is to create one lindked list [[],[],[]] to save
all the collumns with each collumn as a list
and to create one list to save all the nams for collumns
"""


def creat_lis(size):
    name_list = [] * size
    return name_list


def creat_lisoflis(size):
    temp_list = [[] for i in range(size)]
    return temp_list


"""
when iterating through all the data in one license
clean empty columns and save the csv to a new one
"""
def clean_saveas_csv(old_csv_str, new_csv_str):
    data = pd.read_csv(old_csv_str, low_memory=False)
    for col in list(data.columns):
        if "Unnamed" in col:
            data.drop(col, inplace=True, axis=1)
    data.to_csv(new_csv_str)


retries = 0
flickr = flickrapi.FlickrAPI(secret_key.api_key,
                             secret_key.api_secret, format='json')
# below is the cc licenses list
license_list = [1, 2, 3, 4, 5, 6, 9, 10]

# we want to have these 11 columns of data saved in final csv
# name_lis is the header of final table
# temp_list is in the form of list within list, which saves the actual data
# each internal list is a column: ie. temp_list[0] saves the data of id number
name_list = ["id", "dateuploaded", "isfavorite", "license", "realname",
            "location", "title", "description", "dates", "views",
             "comments", "tags"]
temp_list = creat_lisoflis(len(name_list))


while True:
    try:
        # use rec txt to record j(current page), i(current license), and total
        # every time iterating through one page of photos
        # to pick up from where the script errors or stops
        with open('rec.txt') as f:
            readed = f.read().split(" ")
            j = int(readed[0])
            i = int(readed[1])
            total = int(readed[2])
        while i in license_list:
            while j <= total:
                # use search method to pull photo id in each license
                photosJson = flickr.photos.search(license=i,
                                                  per_page=100, page=j)
                time.sleep(1)
                photos = json.loads(photosJson.decode('utf-8'))
                id = [x["id"] for x in photos["photos"]["photo"]]
                print(len(id))

                # change total equals to the total picture number
                # under current license everytime moving to the
                # 1st page of a new license
                # and set the final CSV as empty if is at the 1st page
                if j == 1:
                    total = photos["photos"]["pages"]
                    data = pd.read_csv('final.csv', low_memory=False)
                    for col in list(data.columns):
                        data.drop(col, inplace=True, axis=1)
                    data.to_csv("final.csv")

                # use getInfo method to get more detailed photo
                # info from inputting photo id
                # and query data and save into list (temp_list)
                # as columns of final dataset
                for index in range(0, len(id)):
                    detailJson = flickr.photos.getInfo(license=i,
                                                       photo_id=id[index])
                    time.sleep(1)
                    photos_detail = json.loads(detailJson.decode('utf-8'))
                    print(index, "id out of", len(id), "in license", i,
                          "page", j, "out of", total)

                    # below is the query process of useful data
                    for a in range(0, len(name_list)):
                        # name_list = ["id", "dateuploaded",
                        # "isfavorite", "license", "realname", "location",
                        # "title", "description", "dates", "views", "comments",
                        # "tags"] "tags" is the list of tags for that pic
                        if (a >= 0 and a < 4) or a == 9:
                            temp_list = query(photos_detail, name_list[a],
                                              temp_list, a)
                        if a == 4 or a == 5:
                            temp_list = data_query(photos_detail, "owner",
                                                   name_list[a], temp_list, a)
                        if a == 6 or a == 7 or a == 10:
                            temp_list = data_query(photos_detail, name_list[a],
                                                   "_content", temp_list, a)
                        if a == 8:
                            temp_list = data_query(photos_detail, name_list[a],
                                                   "taken", temp_list, a)
                        if a == 11:

                            # some photo id has more than one subids included
                            # each corresponds to certain tag(s)
                            # therefore we save tags of each id as a list
                            # further clean/query may be needed in analyzing
                            # this column of data
                            tags = photos_detail["photo"]["tags"]["tag"]
                            if tags:
                                temp_list[a].append([tags[num]["raw"] for
                                                     num in range(len(tags))])
                            else:
                                temp_list = data_query(photos_detail,
                                                       name_list[a], "tag",
                                                       temp_list, a)
                    if index % 100 == 0:
                        # prevent laptop from sleeping
                        pyautogui.moveTo(random.randint(50, 300),
                                         random.randint(50, 300))

                j += 1
                print("page", j, "out of", total, "in license", i,
                      "with retry number", retries)

                # now we will put the list of columns into dataframe
                # and save the dataframe into history csv after
                # iterating through each page
                # and merge history CSVs to final CSV
                # and update j(the current page number in txt)
                # note that the map(pd.read_csv) means overwrite data
                # so duplicate issue solved
                df = to_df(temp_list, name_list)
                df.to_csv('hs.csv')
                df = pd.concat(
                    map(pd.read_csv, ['hs.csv', 'final.csv']),
                    ignore_index=True)
                df.to_csv('final.csv')
                with open('rec.txt', 'w') as f:
                    f.write(str(j) + " " + str(i) + " " + str(total))

                # set list to empty everytime after saving the data into
                # the csv file to prevent from saving duplicate data
                temp_list = creat_lisoflis(len(name_list))

                # if current page number has reached the max
                # limit of total pages
                # reset j to 1 and update i to the license in the dictionary
                if j == total + 1 or j > total:
                    clean_saveas_csv("final.csv", "license" + "i" + ".csv")
                    i += 1
                    j = 1
                    while i not in license_list:
                        i += 1
                    with open('rec.txt', 'w') as f:
                        f.write(str(j) + " " + str(i) + " " + str(total))
                    # below is to clear list everytime
                    # before rerun (to prevent duplicate)
                    temp_list = creat_lisoflis(len(name_list))
                    break

    except Exception as e:
        retries += 1
        print(e)
        print("page", j, "out of", total, "in license", i,
              "with retry number", retries)
        # below is to clear list everytime before rerun (prevent duplicate)
        temp_list = creat_lisoflis(len(name_list))
        continue
