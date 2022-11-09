"""
This file is the script of data analysis and visualization
"""

# Standard library
import sys
import traceback
import warnings

# Third-party
from functools import reduce

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import re

warnings.filterwarnings("ignore")
# Third-party
from wordcloud import STOPWORDS, WordCloud  # noqa: E402


def tags_frequency(csv_path, column_names):
    # attribute csv_path is string
    # attribute column_names is a list
    """
    This function is to generate a word cloud
    based on all the tags of each license
    each license one cloud
    """
    df = pd.read_csv(csv_path)
    for column_name in column_names:
        if column_name == "tags":
            list_tags = (df[column_name][0]).strip("]'[").split("', '")

            # Converting string to list
            for row in df[column_name][1:]:
                if str(row).strip("]'[").split("', '"):
                    list_tags += str(row).strip("]'[").split("', '")
        else:
            list2 = []
            for row in df[column_name][1:]:
                if str(row) is not None and str(row) != "" and str(row) != "nan":
                    print(str(row))
                    if "ChineseinUS.org" in str(row):
                        row = "ChineseinUS"
                    list2 += re.split('\s|(?<!\d)[,.](?!\d)', str(row))
    text = ""
    stopwords = set(STOPWORDS)

    # The stop words can be customized based on diff cases
    flickr_customized = {"nan", "https", "href", "rel", "de", "en",
                         "da", "la", "href", "rel", "noreferrer",
                         "nofollow", "ly", "photo"}
    stopwords = stopwords.union(flickr_customized)
    customized = {"p", "d"}
    stopwords = stopwords.union(customized)

    for word in list_tags:
        # Splitting each tag into its constituent words
        tokens = word.split()
        # Converting each word to lower case
        for i in range(len(tokens)):
            tokens[i] = tokens[i].lower()
        # Adding each word to text
        text += " ".join(tokens) + " "
    for word in list2:
        tokens = word.split()
        for j in range(len(tokens)):
            tokens[j] = tokens[j].lower()
        text += " ".join(tokens) + " "

    # Creating the word cloud
    tags_word_cloud = WordCloud(
        width=800,
        height=800,
        background_color="white",
        stopwords=stopwords,
        min_font_size=10,
    ).generate(text)

    # Plotting the word cloud
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(tags_word_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig('../analyze/wordCloud_plots/license9_wordCloud.png', dpi=300, bbox_inches='tight')
    plt.show()


def time_trend_helper(df):
    year_list = []
    for date_row in df['dates'][0:]:
        date_list = str(date_row).split()
        year_list.append(date_list[0])
    df['Dates'] = year_list

    # Use rename_axis for name of column from index and reset_index
    count_df = df['Dates'].value_counts().sort_index(). \
        rename_axis('Dates').reset_index(name="Counts")
    count_df = count_df.drop([0, len(count_df) - 1])
    return count_df


def time_trend(csv_path):
    df = pd.read_csv(csv_path)
    count_df = time_trend_helper(df)

    # first use subplots() to create a frame of your plot (figure and axes)
    fig, ax = plt.subplots(figsize=(10, 5))
    plt.plot(count_df["Dates"], count_df["Counts"])
    plt.xticks(rotation=60)

    # We have 828 time nodes in this dataset.
    # So we start from the 0th time node, and end at 828th time node.
    # and step 90 digits each time - only have the 0th, 90th, 180th, ... time nodes showing on this graph.
    ax.set_xticks(np.arange(0, len(count_df), 90))
    fig.suptitle('license1 usage in flickr pictures 1967-2022', fontweight="bold")
    plt.savefig('../analyze/line_graphs/license1_total_trend.png', dpi=300, bbox_inches='tight')
    plt.show()


def time_trend_compile():
    license1 = pd.read_csv("../flickr/dataset/cleaned_license1.csv")
    license2 = pd.read_csv("../flickr/dataset/cleaned_license2.csv")
    license3 = pd.read_csv("../flickr/dataset/cleaned_license3.csv")
    license4 = pd.read_csv("../flickr/dataset/cleaned_license4.csv")
    license5 = pd.read_csv("../flickr/dataset/cleaned_license5.csv")
    license6 = pd.read_csv("../flickr/dataset/cleaned_license6.csv")
    license9 = pd.read_csv("../flickr/dataset/cleaned_license9.csv")
    count_df1 = time_trend_helper(license1)
    count_df2 = time_trend_helper(license2)
    count_df3 = time_trend_helper(license3)
    count_df4 = time_trend_helper(license4)
    count_df5 = time_trend_helper(license5)
    count_df6 = time_trend_helper(license6)
    count_df9 = time_trend_helper(license9)
    list_raw_data = [count_df1, count_df2, count_df3,
                     count_df4, count_df5, count_df6, count_df9]

    # Split date to year and save in a list
    for each_raw_data in list_raw_data:
        years = []
        for row in each_raw_data["Dates"]:
            years.append(row.split("-")[0])
        each_raw_data["Years"] = years
        each_raw_data = each_raw_data.drop("Dates", axis=1)
        each_raw_data["Counts_by_year"] =\
            each_raw_data["Counts"].groupby(each_raw_data["Years"]).sum()
        each_raw_data.dropna(how='all')
    print(each_raw_data)
    print(count_df9.count())
    # We set years are from 2000 to 2022
    Years = np.arange(2000, 2022)


    # plot lines
    # plt.plot(df_common_dates[0], count_df1["Counts"], label="license 1")
    # plt.plot(df_common_dates[0], count_df2["Counts"], label="license 2")
    # plt.plot(df_common_dates[0], count_df3["Counts"], label="license 3")
    # plt.plot(df_common_dates[0], count_df4["Counts"], label="license 4")
    # plt.plot(df_common_dates[0], count_df5["Counts"], label="license 5")
    # plt.plot(df_common_dates[0], count_df6["Counts"], label="license 6")
    # plt.plot(df_common_dates[0], count_df9["Counts"], label="license 9")
    # plt.legend()
    # plt.savefig('../analyze/line_graphs/licenses_total_trend.png', dpi=300, bbox_inches='tight')
    # plt.show()


def view_compare_helper(df):
    highest_view = int(max(df["views"]))
    df = df.sort_values("views", ascending=False)
    return highest_view
    print(df)
    print(highest_view)


def view_compare():
    license1 = pd.read_csv("../flickr/dataset/cleaned_license1.csv")
    license2 = pd.read_csv("../flickr/dataset/cleaned_license2.csv")
    license3 = pd.read_csv("../flickr/dataset/cleaned_license3.csv")
    license4 = pd.read_csv("../flickr/dataset/cleaned_license4.csv")
    license5 = pd.read_csv("../flickr/dataset/cleaned_license5.csv")
    license6 = pd.read_csv("../flickr/dataset/cleaned_license6.csv")
    license9 = pd.read_csv("../flickr/dataset/cleaned_license9.csv")
    licenses = [license1, license2, license3, license4, license5, license6, license9]
    maxs = []
    for lic in licenses:
        maxs.append(view_compare_helper(lic))
    print(maxs)
    temp_data = pd.DataFrame()
    temp_data["Licenses"] = ["license1", "license2", "license3", "license4", "license5", "license6", "license9"]
    temp_data["views"] = maxs
    fig, ax = plt.subplots(figsize =(10, 7))
    ax.grid(b=True, color='grey',
            linestyle='-.', linewidth=0.5,
            alpha=0.6)
    sns.set_style("dark")
    sns.barplot(data=temp_data, x="Licenses", y="views", palette="flare", errorbar="sd")
    ax.set_title('Maximum Views of All Licenses',
                 loc='left')
    plt.savefig('../analyze/compare_graphs/max_views.png', dpi=300, bbox_inches='tight')
    plt.show()


def heat_map(csv_path):
    df = pd.read_csv(csv_path)
    for i in range(len(df["license"])):
        if df["license"][i] != 1.0:
            df2 = df.drop(i)
    df2 = df2.dropna(how="all")
    print(df2)
    df2 = df2.groupby('location').sum()
    print(df2)


def main():
    view_compare()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        print("INFO (130) Halted via KeyboardInterrupt.", file=sys.stderr)
        sys.exit(130)
    except Exception:
        print("ERROR (1) Unhandled exception:", file=sys.stderr)
        print(traceback.print_exc(), file=sys.stderr)
    sys.exit(1)
