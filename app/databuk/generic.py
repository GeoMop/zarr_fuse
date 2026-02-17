import requests
from requests_cache import CachedSession

def create_http_cache(fname: str, flag: bool):
    """
    Create an HTTP session that is either cached or plain.

    :param fname:  For cache: path (or directory) to use for the cache.
                   For plain: will be ignored.
    :param flag:   If True, return a CachedSession; otherwise a requests.Session.
    :return:       A session-like object with .get(), .post(), etc.
    """
    if flag:
        # Uses fname as cache name (SQLite file or filesystem dir,
        # depending on default backend or your explicit choice)
        return CachedSession(cache_name=fname)
    else:
        # A “strict” session with no caching
        return requests.Session()

