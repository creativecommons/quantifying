"""
This file is the script of data analysis and visualization
"""

# Standard library
import sys
import traceback

# Third-party
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from textblob import TextBlob
import warnings
warnings.filterwarnings("ignore")
from wordcloud import WordCloud, STOPWORDS


def tags_frequency(csv_path, column_name):  # attributes are string
    """
    This function is to generate a word cloud
    based on all the tags of each license
    each license one cloud
    """
    df = pd.read_csv(csv_path)
    list_tags = (df[column_name][0]).strip("]'[").split("', '")

    print(list_tags)
    # Converting string to list
    for row in df[column_name][1:]:
        if row.strip("]'[").split("', '"):
            list_tags += row.strip("]'[").split("', '")
    print(list_tags)
    text = ''
    stopwords = set(STOPWORDS)
    for word in list_tags:
        # Splitting each tag into its constituent words
        tokens = word.split()
        # Converting each word to lower case
        for i in range(len(tokens)):
            tokens[i] = tokens[i].lower()
            # Adding each word to text
            text += " ".join(tokens) + " "

    # Creating the word cloud
    tags_word_cloud = WordCloud(width=800, height=800,
                          background_color='white',
                          stopwords=stopwords,
                          min_font_size=10).generate(text)

    # Plotting the word cloud
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(tags_word_cloud)
    plt.axis("off")

    plt.show()

tags_frequency("cleaned_hs.csv", "tags")