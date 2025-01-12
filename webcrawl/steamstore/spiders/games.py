import json
import math
import urllib.parse as urltools

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector


class GamesSpider(scrapy.Spider):
    name = "games"
    allowed_domains = ["store.steampowered.com"]

    def __init__(self, total_count=None, per_page=None, *args, **kwargs):
        super(GamesSpider, self).__init__(*args, **kwargs)

        self.total_count = total_count
        self.per_page = per_page or 100

    def start_requests(self):
        self.logger.info(
            "Using user-agent: "
            + str(self.settings.attributes.get("USER_AGENT", "None"))
        )

        yield scrapy.Request(
            "https://store.steampowered.com/search/?category1=998&ndl=1&ignore_preferences=1&cc=us&l=english",
            callback=self.parse_search_params,
        )

    def parse_search_params(self, response):
        search_snr = (
            response.css(
                "#hidden_searchform_elements #search_snr_value::attr(value)"
            ).get()
            or ""
        )

        self.logger.info(f"SNR request: {search_snr}")

        if not self.total_count:
            self.logger.debug("Trying to detect number of games automatically...")

            yield scrapy.Request(
                "https://store.steampowered.com/search/results/?query&start=0&count=1&category1=998&ndl=1&"
                f"ignore_preferences=1&sort_by=_ASC&snr={search_snr}&infinite=1&l=english&cc=us",
                callback=self.parse_total_count,
                cb_kwargs={"snr": search_snr},
            )
        else:
            self.logger.debug(f"Collecting number of games: {self.total_count}")

            for page_id in range(math.ceil(self.total_count / self.per_page)):
                self.logger.debug(f"Parsing the page with number: {page_id}")

                yield scrapy.Request(
                    f"https://store.steampowered.com/search/results/?query&start={self.per_page * page_id}&"
                    f"count={self.per_page}&category1=998&ndl=1&ignore_preferences=1&sort_by=_ASC&snr={search_snr}&"
                    "infinite=1&l=english&cc=us",
                    callback=self.parse,
                    cb_kwargs={"snr": search_snr},
                )

    def parse_total_count(self, response, **kwargs):
        total_count = json.loads(response.text).get("total_count")

        if not total_count:
            raise CloseSpider("Failed to retrieve number of games on the store")

        self.logger.debug(f"Total number of games available: {total_count}")

        for page_id in range(math.ceil(total_count / self.per_page)):
            self.logger.debug(f"Parsing the page with number: {page_id}")

            yield scrapy.Request(
                f"https://store.steampowered.com/search/results/?query&start={self.per_page * page_id}&"
                f"count={self.per_page}&category1=998&ndl=1&ignore_preferences=1&sort_by=_ASC&snr={kwargs.get('snr')}&"
                "infinite=1&l=english&cc=us",
                callback=self.parse,
                cb_kwargs={"snr": kwargs.get("snr")},
            )

    def parse(self, response, **kwargs):
        page = json.loads(response.text).get("results_html", "")

        for item in Selector(text=page).css("a"):
            app_href = item.css("::attr(href)").get()
            app_url = urltools.urlparse(app_href)
            app_url_params = urltools.parse_qs(app_url.query)
            app_url_params.update({
                "cc": ["us"],
                "l": ["english"],
                "category1": ["998"],
                "ndl": ["1"],
                "ignore_preferences": ["1"],
                "snr": [kwargs.get("snr")],
            })
            app_url_query = urltools.urlencode(app_url_params, doseq=True)
            app_url_updated = urltools.urlunparse((
                app_url.scheme,
                app_url.netloc,
                app_url.path,
                app_url.params,
                app_url_query,
                app_url.fragment,
            ))

            yield scrapy.Request(app_url_updated, callback=self.parse_game_card)

    def parse_game_card(self, response):
        developer_info = response.css("#game_highlights .dev_row")
        products_info = response.css(".game_area_purchase > div")
        app_id = response.css("*[data-appid]::attr(data-appid)").get()

        self.logger.info(f"Parsing the game page with id={app_id}")

        yield {
            "app_id": app_id,
            "img_href": response.css("#gameHeaderImageCtn img::attr(src)").get(),
            "title": response.css("#appHubAppName::text").get(),
            "about": response.css("#game_highlights .game_description_snippet").get(),
            "description": response.css("#game_area_description").get(),
            "date_release": response.css(
                "#game_highlights .release_date .date::text"
            ).get(),
            "products": [
                {
                    "name": product.css("h1::text").get(),
                    "platforms": [
                        platform.replace("platform_img ", "")
                        for platform in product.css(
                            ".game_area_purchase_platform span::attr(class)"
                        ).getall()
                    ],
                    "price": product.css(
                        ".game_purchase_action div[data-price-final]::attr(aria-label)"
                    ).get(),
                }
                for product in products_info
            ],
            "rating": response.css(
                "#userReviews"
                " .user_reviews_summary_row:last-child::attr(data-tooltip-html)"
            ).get(),
            "developer": developer_info.css(".summary#developers_list").get(),
            "publisher": developer_info.css(".summary:not(#developers_list)").get(),
            "tags": response.css("#glanceCtnResponsiveRight a.app_tag::text").getall(),
        }
