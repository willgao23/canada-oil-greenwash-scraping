WAYBACK_ENDPOINT = "http://web.archive.org/cdx/search/cdx"
WAYBACK_PREFIX = "http://web.archive.org/web"
BILL_C59_ROYAL_ASSENT_DATE = 20240620000000
URLS = {
    "Suncor Energy": {
        "current": "https://www.suncor.com/en-ca/news-and-stories/news-releases"
    },
    "Pembina Pipeline": {
        "current": "https://www.pembina.com/media-centre/news-releases"
    },
    "Imperial Oil": {
        "current": "https://news.imperialoil.ca/news-releases/default.aspx"
    },
    "Enbridge": {"current": "https://www.enbridge.com/media-center/news"},
    "Canadian Natural Resources": {
        "current": "https://www.cnrl.com/investors/news-releases/"
    },
    "Shell Canada": {"current": "https://www.shell.ca/en_ca/media/news-releases.html"},
}
LINK_CSV_FIELDS = ["Organization", "Link", "Date Scraped", "Type"]
