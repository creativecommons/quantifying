"""
This file is the script of data analysis and visualization
"""

# Standard library
import os
import re
import sys
import traceback
import warnings

# Third-party
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import seaborn as sns
from wordcloud import STOPWORDS, WordCloud  # noqa: E402

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Warning suppression /!\ Caution /!\
warnings.filterwarnings("ignore")

# Setup PATH_WORK_DIR, and LOGGER using quantify.setup()
_, PATH_WORK_DIR, _, _, LOGGER = quantify.setup(__file__)


def tags_frequency(csv_path, column_names):
    """
    Generate a word cloud based on all the tags of each license.
    Each license has its own cloud.

    Args:
    - csv_path (str): Path to the CSV file containing data.
    - column_names (list): List of column names to process.
                           Example: ["tags", "description"]

    """
    LOGGER.info("Generating word cloud based on tags.")

    df = pd.read_csv(csv_path)
    # Process each column containing tags
    for column_name in column_names:
        list2 = []
        if column_name == "tags":
            list_tags = (df[column_name][0]).strip("]'[").split("', '")

            # Converting string to list
            for row in df[column_name][1:]:
                if str(row).strip("]'[").split("', '"):
                    list_tags += str(row).strip("]'[").split("', '")
        else:
            for row in df[column_name][1:]:
                if (
                    str(row) is not None
                    and str(row) != ""
                    and str(row) != "nan"
                ):
                    LOGGER.debug(f"Processing row: {row}")
                    if "ChineseinUS.org" in str(row):
                        row = "ChineseinUS"
                    list2 += re.split(r"\s|(?<!\d)[,.](?!\d)", str(row))
    text = ""
    stopwords = set(STOPWORDS)

    # Customize stop words for the word cloud
    flickr_customized = {
        "nan",
        "https",
        "href",
        "rel",
        "de",
        "en",
        "et",
        "un",
        "el",
        "le",
        "un",
        "est",
        "à",
        "lo",
        "da",
        "la",
        "href",
        "rel",
        "noreferrer",
        "nofollow",
        "ly",
        "photo",
        "qui",
        "que",
        "dan",
        "pa",
        "ou",
        "quot",
        "rolandtanglaophoto",
    }
    stopwords = stopwords.union(flickr_customized)
    # customized = {"p", "d", "b"}
    # stopwords = stopwords.union(customized)

    # Initialize an empty list to store lowercase words
    lowercase_words = []

    # Iterate over each tag in list_tags and list2
    for tag in list_tags + list2:
        # Split the tag into words, convert to lowercase,& append to the list.
        lowercase_words.extend([word.lower() for word in tag.split()])

    # Join the lowercase words with a space separator
    text = " ".join(lowercase_words)

    # Creating WordCloud
    tags_word_cloud = WordCloud(
        width=800,
        height=800,
        background_color="white",
        stopwords=stopwords,
        min_font_size=10,
    ).generate(text)

    # Plotting the WordCloud
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(tags_word_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(
        "Flickr Photos under Creative Commons Licenses: Categories Keywords",
        fontweight="bold",
    )
    plt.savefig(
        os.path.join(PATH_WORK_DIR, "wordCloud_plots/license1_wordCloud.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()


def time_trend_helper(df):
    """
    Extract year-wise count of entries from a DataFrame.

    Args:
    - df (DataFrame): Input DataFrame containing dates.

    Returns:
    - DataFrame: DataFrame with counts of entries per year.
    """
    LOGGER.info("Extracting year-wise count of entries.")

    year_list = []
    for date_row in df["dates"][0:]:
        date_list = str(date_row).split()
        year_list.append(date_list[0])
    df["Dates"] = year_list
    # Count occurrences of each year
    # Use rename_axis for name of column from index and reset_index
    count_df = (
        df["Dates"]
        .value_counts()
        .sort_index()
        .rename_axis("Dates")
        .reset_index(name="Counts")
    )
    # Remove first and last rows
    count_df = count_df.drop([0, len(count_df) - 1])
    return count_df


def time_trend(csv_path):
    """
    Generate a line graph to show the time trend of the license usage.

    Args:
    - csv_path (str): Path to the CSV file.
    """
    LOGGER.info("Generating time trend line graph.")

    df = pd.read_csv(csv_path)
    count_df = time_trend_helper(df)

    # first use subplots() to create a frame of your plot (figure and axes)
    fig, ax = plt.subplots(figsize=(10, 5))
    plt.plot(count_df["Dates"], count_df["Counts"], alpha=0.5)
    plt.xticks(rotation=60)

    # We have 828 time nodes in this dataset.
    # So we start from the 0th time node, and end at 828th time node.
    # and step 90 digits each time - only have the 0th, 90th,
    # 180th, ... time nodes showing on this graph.
    ax.set_xticks(np.arange(0, len(count_df), 100))
    # ["CC BY-NC-SA 2.0", "CC BY-NC 2.0", "CC BY-NC-ND 2.0", "CC BY 2.0",
    #  "CC BY-SA 2.0", "CC BY-ND 2.0", "CC0 1.0", "Public Domain Mark 1.0"]
    plt.title("Data range: first 4000 pictures", fontsize=13)
    plt.suptitle(
        "CC BY-SA 2.0 license usage in flickr pictures taken during 1962-2022",
        fontsize=15,
        fontweight="bold",
    )
    plt.xlabel("Day", fontsize=10)
    plt.ylabel("Amount", fontsize=10)
    plt.savefig(
        os.path.join(PATH_WORK_DIR, "line_graphs/license5_total_trend.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()


def time_trend_compile_helper(yearly_count):
    """
    Filter yearly trend data for the years between 2018 and 2022.

    Args:
    - yearly_count (DataFrame): DataFrame with "year" and "Counts" columns.

    Returns:
    - DataFrame: Filtered yearly count data.
    """
    LOGGER.info("Filtering yearly trend data.")

    Years = np.arange(2018, 2023)
    yearly_count["year"] = list(yearly_count.index)
    counts = []
    for num in range(len(yearly_count["Counts"])):
        if (int(yearly_count["year"][num]) <= 2022) & (
            int(yearly_count["year"][num]) >= 2018
        ):
            counts.append(yearly_count["Counts"][num])
    LOGGER.info(f"{counts}")
    final_yearly_count = pd.DataFrame(
        list(zip(Years, counts)), columns=["Years", "Yearly_counts"]
    )
    return final_yearly_count


def time_trend_compile():
    """
    Compile yearly trends for different licenses and plot them.
    """
    LOGGER.info("Compiling yearly trends for different licenses.")

    license1 = pd.read_csv("../flickr/dataset/cleaned_license1.csv")
    license2 = pd.read_csv("../flickr/dataset/cleaned_license2.csv")
    license3 = pd.read_csv("../flickr/dataset/cleaned_license3.csv")
    license4 = pd.read_csv("../flickr/dataset/cleaned_license4.csv")
    license5 = pd.read_csv("../flickr/dataset/cleaned_license5.csv")
    license6 = pd.read_csv("../flickr/dataset/cleaned_license6.csv")
    license9 = pd.read_csv("../flickr/dataset/cleaned_license9.csv")
    license10 = pd.read_csv("../flickr/dataset/cleaned_license10.csv")
    # Calculate yearly counts for each license
    count_df1 = time_trend_helper(license1)
    count_df2 = time_trend_helper(license2)
    count_df3 = time_trend_helper(license3)
    count_df4 = time_trend_helper(license4)
    count_df5 = time_trend_helper(license5)
    count_df6 = time_trend_helper(license6)
    count_df9 = time_trend_helper(license9)
    count_df10 = time_trend_helper(license10)
    list_raw_data = [
        count_df1,
        count_df2,
        count_df3,
        count_df4,
        count_df5,
        count_df6,
        count_df9,
        count_df10,
    ]

    # Split date to year and save in a list
    list_data = []
    for each_raw_data in list_raw_data:
        years = []
        for row in each_raw_data["Dates"]:
            years.append(row.split("-")[0])
        each_raw_data["Years"] = years
        each_raw_data = each_raw_data.drop("Dates", axis=1)
        each_raw_data = each_raw_data.groupby("Years")["Counts"].sum()
        each_raw_data.dropna(how="all")
        list_data.append(each_raw_data)

    yearly_count1 = list_data[0].to_frame()
    yearly_count2 = list_data[1].to_frame()
    yearly_count3 = list_data[2].to_frame()
    yearly_count4 = list_data[3].to_frame()
    yearly_count5 = list_data[4].to_frame()
    yearly_count6 = list_data[5].to_frame()
    yearly_count9 = list_data[6].to_frame()
    yearly_count10 = list_data[7].to_frame()
    # Filter yearly count data for the years between 2018 and 2022
    yearly_count1 = time_trend_compile_helper(yearly_count1)
    yearly_count2 = time_trend_compile_helper(yearly_count2)
    yearly_count3 = time_trend_compile_helper(yearly_count3)
    yearly_count4 = time_trend_compile_helper(yearly_count4)
    yearly_count5 = time_trend_compile_helper(yearly_count5)
    yearly_count6 = time_trend_compile_helper(yearly_count6)
    yearly_count9 = time_trend_compile_helper(yearly_count9)
    yearly_count10 = time_trend_compile_helper(yearly_count10)
    LOGGER.info(f"{yearly_count1}")

    # Plot yearly trend for all licenses
    plt.plot(
        yearly_count1["Years"],
        yearly_count1["Yearly_counts"],
        label="CC BY-NC-SA 2.0",
        alpha=0.7,
        linestyle="-",
    )
    plt.plot(
        yearly_count2["Years"],
        yearly_count2["Yearly_counts"],
        label="CC BY-NC 2.0",
        alpha=0.7,
        linestyle="--",
    )
    plt.plot(
        yearly_count3["Years"],
        yearly_count3["Yearly_counts"],
        label="CC BY-NC-ND 2.0",
        alpha=0.7,
        linestyle="-.",
    )
    plt.plot(
        yearly_count4["Years"],
        yearly_count4["Yearly_counts"],
        label="CC BY 2.0",
        alpha=0.7,
        linestyle=":",
    )
    plt.plot(
        yearly_count5["Years"],
        yearly_count5["Yearly_counts"],
        label="CC BY-SA 2.0",
        alpha=0.7,
        linestyle="-",
    )
    plt.plot(
        yearly_count6["Years"],
        yearly_count6["Yearly_counts"],
        label="CC BY-ND 2.0",
        alpha=0.7,
        linestyle="--",
    )
    plt.plot(
        yearly_count9["Years"],
        yearly_count9["Yearly_counts"],
        label="CC0 1.0",
        alpha=0.7,
        linestyle=":",
    )
    plt.plot(
        yearly_count10["Years"],
        yearly_count10["Yearly_counts"],
        label="Public Domain Mark 1.0",
        alpha=0.7,
    )
    plt.legend()
    plt.xlabel("Date of photos taken", fontsize=10)
    plt.ylabel("Amount of photos", fontsize=10)
    plt.title(
        "Data range: first 4000 pictures for each license",
        fontsize=13,
        alpha=0.75,
    )
    plt.suptitle(
        "Yearly Trend of All Licenses 2018-2022",
        fontsize=15,
        fontweight="bold",
    )
    plt.savefig(
        "../analyze/line_graphs/licenses_yearly_trend.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()


def view_compare_helper(df):
    """
    Calculate maximum views of pictures under a license.

    Args:
    - df (DataFrame): Input DataFrame.

    Returns:
    - int: Maximum views.
    """
    LOGGER.info("Calculating maximum views of pictures under a license.")

    highest_view = int(max(df["views"]))
    df = df.sort_values("views", ascending=False)
    LOGGER.info(f"DataFrame sorted by views in descending order: {df}")
    LOGGER.info(f"Maximum views found: {highest_view}")
    return highest_view


def view_compare():
    """
    Compare maximum views of pictures under different licenses.
    """
    LOGGER.info(
        "Comparing maximum views of pictures under different licenses."
    )

    license1 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license1.csv")
    )
    license2 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license2.csv")
    )
    license3 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license3.csv")
    )
    license4 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license4.csv")
    )
    license5 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license5.csv")
    )
    license6 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license6.csv")
    )
    license9 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license9.csv")
    )
    license10 = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/cleaned_license10.csv")
    )
    licenses = [
        license1,
        license2,
        license3,
        license4,
        license5,
        license6,
        license9,
        license10,
    ]
    # Calculate maximum views for each license
    maxs = []
    for lic in licenses:
        maxs.append(view_compare_helper(lic))
    LOGGER.info(f"{maxs}")
    # Create DataFrame to store license and their maximum views
    temp_data = pd.DataFrame()
    temp_data["Licenses"] = [
        "CC BY-NC-SA 2.0",
        "CC BY-NC 2.0",
        "CC BY-NC-ND 2.0",
        "CC BY 2.0",
        "CC BY-SA 2.0",
        "CC BY-ND 2.0",
        "CC0 1.0",
        "Public Domain Mark 1.0",
    ]
    temp_data["views"] = maxs
    # Plot bar graph
    fig, ax = plt.subplots(figsize=(13, 10))
    ax.grid(b=True, color="grey", linestyle="-.", linewidth=0.5, alpha=0.6)
    sns.set_style("dark")
    sns.barplot(
        data=temp_data, x="Licenses", y="views", palette="flare", errorbar="sd"
    )
    ax.bar_label(ax.containers[0])
    ax.text(
        x=0.5,
        y=1.1,
        s="Maximum Views of Pictures under all Licenses",
        fontsize=15,
        weight="bold",
        ha="center",
        va="bottom",
        transform=ax.transAxes,
    )
    ax.text(
        x=0.5,
        y=1.05,
        s="Data range: first 4000 pictures for each license",
        fontsize=13,
        alpha=0.75,
        ha="center",
        va="bottom",
        transform=ax.transAxes,
    )
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticklabels(["{:,.0f}".format(x) for x in current_values])
    plt.savefig(
        os.path.join(PATH_WORK_DIR, "../analyze/compare_graphs/max_views.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()


def total_usage():
    """
    Generate a bar plot showing the total usage of different licenses.
    """
    LOGGER.info(
        "Generating bar plot showing total usage of different licenses."
    )

    # Reads the license total file as the input dataset
    df = pd.read_csv(
        os.path.join(PATH_WORK_DIR, "../flickr/dataset/license_total.csv")
    )
    df["License"] = [str(x) for x in list(df["License"])]
    fig = px.bar(df, x="License", y="Total amount", color="License")
    fig.write_html(os.path.join(PATH_WORK_DIR, "../analyze/total_usage.html"))
    # fig.show()


def main():
    tags_frequency(
        os.path.join(PATH_WORK_DIR, "merged_all_cleaned.csv"), ["tags"]
    )
    # df = pd.read_csv("../flickr/dataset/cleaned_license10.csv")
    # print(df.shape)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
