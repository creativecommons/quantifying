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
import sys
import time

# Third-party
import traceback

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
This function is to save the data first into datafram and then csv
temp_csv is the csv that used for saving data every 100 seconds
temp_csv is set to prevent data from losing when script stops
final_csv is the final csv for one certain license
pd.concat(map...) means to merge temp CSV to final CSV
both temp_csv and final_csv should be path in form of string
note that the map(pd.read_csv) means overwrite data
so duplicate issue solved
"""
def df_to_csv(temp_list, name_list, temp_csv, final_csv):
    df = to_df(temp_list, name_list)
    df.to_csv(temp_csv)
    df = pd.concat(map(pd.read_csv, [temp_csv, final_csv]),
                   ignore_index=True)
    df.to_csv(final_csv)


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


"""
This is the function to query the useful data
from raw pulled data
in our case useful data is supposed to be this
name lise: ["id", "dateuploaded", "isfavorite",
"license", "realname", "location", "title",
"description", "dates", "views", "comments", "tags"]
"""
def query_data(raw_data, name_list, data_list):
    for a in range(0, len(name_list)):
        if (a >= 0 and a < 4) or a == 9:
            data_list = query(raw_data, name_list[a],
                              data_list, a)
        elif a == 4 or a == 5:
            data_list = data_query(raw_data, "owner",
                                   name_list[a], data_list, a)
        elif a == 6 or a == 7 or a == 10:
            data_list = data_query(raw_data, name_list[a],
                                   "_content", data_list, a)
        elif a == 8:
            data_list = data_query(raw_data, name_list[a],
                                   "taken", data_list, a)

        # some photo id has more than one subids included
        # each corresponds to certain tag(s)
        # therefore we save tags of each id as a list
        # further clean/query may be needed in analyzing
        # this column of data
        if a == 11:
            tags = raw_data["photo"]["tags"]["tag"]
            if tags:
                data_list[a].append([tags[num]["raw"] for
                                     num in range(len(tags))])
            else:
                data_list = data_query(raw_data,
                                       name_list[a], "tag",
                                       data_list, a)


"""
This function is to prevent the laptop from sleeping
if we want to pull HUGE amount of data automatically.
The mouse will be moved to a random place without clicking
everytime after certain seconds.
If we want to move mouse after pulling 100 api calls,
then the seconds should be 100, and total_number should be
the current total number of api calls (i.e. the ID amount we have iterated through)
"""
def automove_mouse (total_number, seconds):
    if total_number % seconds == 0:
        pyautogui.moveTo(random.randint(50, 300),
                         random.randint(50, 300))

"""
change total equals to the total picture number under current license
everytime moving to the 1st page of a new license
and set the final CSV as empty if is at the 1st page
final_csv is the path in the form of string
"""
def page1_reset(final_csv, raw_data):
    data = pd.read_csv(final_csv, low_memory=False)
    for col in list(data.columns):
        data.drop(col, inplace=True, axis=1)
        data.to_csv(final_csv)
    return raw_data["photos"]["pages"]


def main():
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
                # and set the final CSV as empty
                if j == 1:
                    total = page1_reset("final.csv", photos)

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

                    # query process of useful data
                    query_data(photos_detail, name_list, temp_list)
                    automove_mouse (index, 100)

                j += 1
                print("page", j, "out of", total, "in license", i,
                      "with retry number", retries)

                # save csv
                df_to_csv(temp_list, name_list, "hs.csv", "final.csv")
                # update j(the current page number in txt)
                with open('rec.txt', 'w') as f:
                    f.write(str(j) + " " + str(i) + " " + str(total))

                # set list to empty everytime after saving the data into
                # the csv file to prevent from saving duplicate data
                temp_list = creat_lisoflis(len(name_list))

                # if current page has reached the max limit of total pages
                # reset j to 1 and update i to the license in the dictionary
                if j == total + 1 or j > total:
                    clean_saveas_csv("final.csv", "license" + str(i) + ".csv")
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


if __name__ == "__main__":
    try:
        main()
    except Exception:
        main.retries += 1
        # below is to clear list everytime before rerun (prevent duplicate)
        main.temp_list = creat_lisoflis(len(main.name_list))
        print("page", j, "out of", main.total, "in license", main.i,
              "with retry number", main.retries)
        print("ERROR (1) Unhandled exception:", file=sys.stderr)
        print(traceback.print_exc(), file=sys.stderr)
        continue