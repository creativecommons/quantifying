# Data Sources

This project uses data from various sources that are openly licensed or in the public domain. Below are the sources and their respective information:

## CC Legal Tools

**Description:** A .txt provided by Timid Robot containing all legal tool paths. The data from Google Custom Search will only cover 50+ general, most significant categories of CC License for data collection quota constraint. As an additional note, the order of precedence of license the collected data's first column is sorted due to intermediate data analysis progress.

**API documentation link:** [List of all current CC legal tool paths by TimidRobot](https://github.com/creativecommons/quantifying/blob/main/google_custom_search/legal-tool-paths.txt)

**API information:**
- No API key required
- No query limits

## Flickr

**Description:** The Flickr API exposes identifiers for users, photos, photosets and other uniquely identifiable objects.

**API documentation link:** [API documentation - Flickr Services](https://www.flickr.com/services/api/)

**API information:**
- API key required
- Query limit: 3600 requests per hour
- Data available through CSV format

## Google Custom Search JSON API

**Description:** The Custom Search JSON API allows user-defined detailed query and access towards related query data using a programmable search engine.

**API documentation links:**
- [Custom Search JSON API Reference | Programmable Search Engine | Google Developers](https://developers.google.com/custom-search/v1/reference/rest)
- [Method: cse.list | Custom Search JSON API | Google Developers](https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list)

**API information:**
- API key required
- Query limit: 100 queries per day for free version
- Data available through JSON format

## Internet Archive Python Interface

**Description:** A python interface to archive.org to achieve API requests towards internet archive.

**API documentation link:** [internetarchive.Search - Internetarchive: A Python Interface to archive.org](https://internetarchive.readthedocs.io/en/latest/api.html#internetarchive.Search)

**API information:**
- No API key required
- No query limits

## The Metropolitan Museum of Art Collection API

- Description: An API endpoint for receiving Metropolitan Muesum of Art Collection's CC-Licensed works.
- API documentation link: [Latest Updates | The Metropolitan Museum of Art Collection API](https://metmuseum.github.io/)
- API information:
  - No API key required
  - No query limits

## Vimeo API

 Description: The Vimeo API allows users to perform filtered, advanced search on Vimeo videos.
- API documentation link: [Getting Started with the Vimeo API](https://developer.vimeo.com/api/start)
- API information:
  - API key required
  - Query limit: 5000 authenticated requests per day
  - Data available through JSON format

## MediaWiki Action API

- Description: The MediaWiki Action API is a web service that allows access to some wiki features like authentication, page operations, and search. It can provide meta information about the wiki and the logged-in user.
- API documentation link: [MediaWiki Action API](https://www.mediawiki.org/wiki/API:Main_page)
- API information:
  - No API key required
  - Query limit: depends on user status and request type
  - Data available through XML or JSON format
 
## YouTube Data API

- Description: An API from YouTube for platform users to upload videos, adjust video parameters, and obtain search results.
- API documentation link: [Search: list | YouTube Data API | Google Developers](https://developers.google.com/youtube/v3/docs/search/list)
- API information:
  - API key required
  - Query limit: depends on the type and number of requests
  - Data available through JSON format
