# scripts/wikipedia_fetch.py

import requests
from typing import Dict

WIKI_API = "https://en.wikipedia.org/w/api.php"

# Add a User-Agent to avoid 403 errors
HEADERS = {
    "User-Agent": "QuantifyingCommonsBot/1.0 (https://github.com/YOUR_USERNAME/quantifying)"
}


def get_site_statistics() -> Dict[str, int]:
    """
    Fetch general statistics from Wikipedia.

    Returns:
        dict: Dictionary containing:
            - articles: number of articles
            - pages: total number of pages
            - edits: total number of edits
            - users: total number of users
            - images: total number of images
    """
    params = {
        "action": "query",
        "meta": "siteinfo",
        "siprop": "statistics",
        "format": "json"
    }
    try:
        response = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        stats = response.json()['query']['statistics']
        return {
            "articles": stats.get("articles", 0),
            "pages": stats.get("pages", 0),
            "edits": stats.get("edits", 0),
            "users": stats.get("users", 0),
            "images": stats.get("images", 0)
        }
    except requests.RequestException as e:
        print(f"Error fetching Wikipedia site statistics: {e}")
        return {"articles": 0, "pages": 0, "edits": 0, "users": 0, "images": 0}


def search_articles_count(keyword: str) -> int:
    """
    Count the number of Wikipedia articles containing a specific keyword.

    Args:
        keyword (str): Keyword or phrase to search for.

    Returns:
        int: Total number of search hits/articles.
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": keyword,
        "format": "json"
    }
    try:
        response = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.json()['query']['searchinfo']['totalhits']
    except requests.RequestException as e:
        print(f"Error searching Wikipedia articles for '{keyword}': {e}")
        return 0


def fetch_cc_related_statistics() -> Dict[str, int]:
    """
    Fetch statistics related to Creative Commons on Wikipedia.

    Returns:
        dict: Dictionary containing counts of articles referencing CC licenses.
    """
    keywords = [
        "Creative Commons",
        "CC BY",
        "CC BY-SA",
        "CC BY-ND",
        "CC BY-NC",
        "CC BY-NC-SA",
        "CC BY-NC-ND"
    ]
    results = {}
    for kw in keywords:
        results[kw] = search_articles_count(kw)
    return results


if __name__ == "__main__":
    print("Wikipedia Site Statistics:")
    print(get_site_statistics())

    print("\nCreative Commons Related Articles Count:")
    cc_stats = fetch_cc_related_statistics()
    for k, v in cc_stats.items():
        print(f"{k}: {v}")

