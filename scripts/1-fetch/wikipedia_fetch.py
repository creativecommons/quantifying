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
            - pages: number of pages
            - edits: number of edits
            - users: number of users
            - images: number of images
    """
    params = {
        "action": "query",
        "meta": "siteinfo",
        "siprop": "statistics|rightsinfo",
        "format": "json"
    }

    response = requests.get(WIKI_API, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()

    stats = data.get("query", {}).get("statistics", {})

    return {
        "articles": stats.get("articles", 0),
        "pages": stats.get("pages", 0),
        "edits": stats.get("edits", 0),
        "users": stats.get("users", 0),
        "images": stats.get("images", 0)
    }


def search_articles_by_license(license_keyword: str, limit: int = 10) -> Dict[str, int]:
    """
    Search Wikipedia articles containing a specific Creative Commons license keyword.

    Args:
        license_keyword (str): e.g., "CC BY-SA 4.0"
        limit (int): Number of results to fetch

    Returns:
        dict: Dictionary with count and sample articles
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": license_keyword,
        "srlimit": limit,
        "format": "json"
    }

    response = requests.get(WIKI_API, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()

    search_results = data.get("query", {}).get("search", [])
    return {
        "count": len(search_results),
        "sample_titles": [item["title"] for item in search_results]
    }


if __name__ == "__main__":
    print("Wikipedia Site Statistics:")
    stats = get_site_statistics()
    print(stats)

    license_query = "Creative Commons"
    print(f"\nArticles mentioning '{license_query}':")
    results = search_articles_by_license(license_query, limit=5)
    print(results)



