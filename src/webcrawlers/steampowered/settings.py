BOT_NAME = "steampowered"

SPIDER_MODULES = ["steampowered.spiders"]
NEWSPIDER_MODULE = "steampowered.spiders"

LOG_LEVEL = "INFO"

USER_AGENT = "Twisted Web Client"

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 16
CONCURRENT_REQUESTS_PER_IP = 16

DOWNLOAD_DELAY = 3

DOWNLOADER_MIDDLEWARES = {
    "steampowered.middlewares.SteampoweredDownloaderMiddleware": 543,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
