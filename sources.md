# Data Sources

This project uses data from various sources that are openly licensed or in the
public domain. Below are the sources and their respective information:


## CC Legal Tools

**Description:** A `.txt` file provided by Timid Robot containing all legal
tool paths.

**API documentation link:**
- [`google_custom_search/legal-tool-paths.txt`][tools-paths]: a list of all
  current CC legal tool paths by TimidRobot)

**API information:**
- No API key required
- No query limits

[tools-paths]:google_custom_search/legal-tool-paths.txt


## Flickr

**Description:** _With over 5 billion photos (many with valuable metadata such
as tags, geolocation, and Exif data), the Flickr community creates wonderfully
rich data. The Flickr API is how you can access that data. In fact, almost all
the functionality that runs flickr.com is available through the API._ ([Flickr:
The Flickr Developer Guide](https://www.flickr.com/services/developer/))

**API documentation link:**
- [API documentation - Flickr Services](https://www.flickr.com/services/api/)

**API information:**
- API key required
- Query limit: 3600 requests per hour
- Data available through CSV format

## GitHub

**Description:** A development platform for hosting and managing code.

**API documentation link:**
- [GitHub REST API v3](https://docs.github.com/en/rest)

**API information:**
- API key not required but recommended by GitHub
- Query limit: 60 requests per hour if unauthenticated,
  5000 requests per hour if authenticated
- Data available through JSON format


## Google Custom Search JSON API

**Description:** The Custom Search JSON API allows user-defined detailed query
and access towards related query data using a programmable search engine.

**API documentation links:**
- [Custom Search JSON API Reference | Programmable Search Engine | Google
  Developers][google-json]
- [Method: cse.list | Custom Search JSON API | Google Developers][cse-list]

**API information:**
- API key required
- Query limit: 100 queries per day for free version
- Data available through JSON format

**Notes:**
- The data from Google Custom Search will only cover 50+ general, most
  significant categories of CC License for data collection quota constraint.
  As an additional note, the order of precedence of license the collected
  data's first column is sorted due to intermediate data analysis progress.

[google-json]: https://developers.google.com/custom-search/v1/reference/rest
[cse-list]: https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list


## Internet Archive Python Interface

**Description:** A python interface to archive.org to achieve API requests
towards internet archive.

**API documentation link:**
- [internetarchive.Search - Internetarchive: A Python Interface to
  archive.org][ia-search]

**API information:**
- No API key required
- No query limits

[ia-search]: https://internetarchive.readthedocs.io/en/stable/internetarchive.html#internetarchive.Search


## MediaWiki Action API

**Description:** _The MediaWiki Action API is a web service that allows access
to some wiki features like authentication, page operations, and search. It can
provide meta information about the wiki and the logged-in user._ ([API:Main
page - MediaWiki](https://www.mediawiki.org/wiki/API:Main_page))

**API documentation link:**
- [MediaWiki Action API](https://www.mediawiki.org/wiki/API:Main_page)

**API information:**
  - No API key required
  - Query limit: depends on user status and request type
  - Data available through XML or JSON format


## The Metropolitan Museum of Art Collection API

**Description:** _The Met’s Open Access datasets are available through our API.
The API (RESTful web service in JSON format) gives access to all of The Met’s
Open Access data and to corresponding high resolution images (JPEG format) that
are in the public domain._ ([The Metropolitan Museum of Art Collection
API](https://metmuseum.github.io/))

**API documentation link:**
- [Latest Updates | The Metropolitan Museum of Art Collection
  API](https://metmuseum.github.io/)

**API information:**
  - No API key required
  - 80 queries per second


## Vimeo API

**Description:** The Vimeo API allows users to perform filtered, advanced
search on Vimeo videos.

**API documentation link:**
- [Getting Started with the Vimeo API](https://developer.vimeo.com/api/start)

**API information:**
  - API key required
  - Query limit: 5000 authenticated requests per day
  - Data available through JSON format


## YouTube Data API

**Description:** An API from YouTube for platform users to upload videos,
adjust video parameters, and obtain search results.

**API documentation link:**
- [Search: list | YouTube Data API | Google
  Developers](https://developers.google.com/youtube/v3/docs/search/list)

**API information:**
  - API key required
  - Query limit: depends on the type and number of requests
  - Data available through JSON format
