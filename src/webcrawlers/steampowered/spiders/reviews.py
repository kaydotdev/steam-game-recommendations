import json
from distutils.util import strtobool
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
from scrapy.selector import Selector


class GameswithReviewsSpider(scrapy.Spider):
    name = "reviews"
    allowed_domains = ["store.steampowered.com", "steamcommunity.com"]

    def __init__(self, skip_games=0, games_per_page=50, only_games=False, filter_language=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.filter_language = filter_language or "all"
        self.skip_games = int(skip_games)
        self.games_per_page = int(games_per_page)
        self.only_games = bool(strtobool(only_games)) if isinstance(only_games, str) else bool(only_games)

    @staticmethod
    def cast_or_default(val, to_type, default=None):
        try:
            return to_type(val)
        except (ValueError, TypeError):
            return default

    def start_requests(self):
        yield scrapy.Request("https://store.steampowered.com/search/?category1=998&ndl=1&ignore_preferences=1&cc=us&l=english", callback=self.parse_search_params)

    def parse_search_params(self, response):
        snr = response.css("#hidden_searchform_elements #search_snr_value::attr(value)").get()
        self.logger.info(f"Extracted a fresh SNR mark: {snr}")

        yield scrapy.Request(f"https://store.steampowered.com/search/results/?query&start=0&count={self.games_per_page}&dynamic_data=&category1=998&sort_by=_ASC&snr={snr}&infinite=1&cc=us&l=english", cb_kwargs={ "snr": snr })

    def parse(self, response, **kwargs):
        snr = kwargs.get("snr")
        raw_games_number_value = json.loads(response.text).get("total_count")
        games_number = int(self.cast_or_default(raw_games_number_value, int, default=0))

        skip_pages_number = self.skip_games // self.games_per_page
        pages_number = games_number // self.games_per_page

        user_agent = self.settings.attributes.get("USER_AGENT")
        user_agent_val = user_agent.value if user_agent is not None else "None"

        self.logger.info(f"Using 'USER-AGENT' from settings: {user_agent_val}")
        self.logger.info(f"Crawling {pages_number} pages ({self.games_per_page} games per page, {games_number} games in total)")
        self.logger.info(f"Skipping {skip_pages_number} pages ({self.skip_games} games)")

        for i in range(skip_pages_number, pages_number):
            yield scrapy.Request(f"https://store.steampowered.com/search/results/?query&start={self.games_per_page * i}&count={self.games_per_page}&dynamic_data=&category1=998&sort_by=_ASC&snr={snr}&infinite=1&cc=us&l=english",
                                 callback=self.parse_items, cb_kwargs={ "snr": snr })

    def parse_items(self, response, **kwargs):
        snr = kwargs.get("snr")

        for item in Selector(text=json.loads(response.text).get("results_html")).css("a"):
            app_id = self.cast_or_default(item.css("::attr(data-ds-appid)").get(), int)
            app_href = item.css("::attr(href)").get()
            tag_ids = item.css("::attr(data-ds-tagids)").get()
            platforms = item.css("div.responsive_search_name_combined div.search_name div span.platform_img::attr(class)").getall()
            price_final = item.css("div.search_price_discount_combined::attr(data-price-final)").get()

            yield {
                "type": "game",
                "content": {
                    "app_id": app_id,
                    "tag_ids": [self.cast_or_default(x, int, default=0) for x in json.loads(tag_ids)] if tag_ids is not None else [],
                    "app_href": app_href,
                    "img_href": item.css("div.col.search_capsule img::attr(src)").get(),
                    "title": item.css("span.title::text").get(),
                    "platforms": [platform.replace("platform_img ", "") for platform in platforms] if platforms is not None else [],
                    "date_release": item.css("div.search_released::text").get(),
                    "summary": item.css("div.search_reviewscore span.search_review_summary::attr(data-tooltip-html)").get(),
                    "price_final": self.cast_or_default(price_final, int),
                    "price_raw": item.css("div.search_price_discount_combined div.search_price").get(),
                    "price_discount_raw": item.css("div.search_price_discount_combined div.search_discount").get(),
                    "steam_deck_compatible": item.css("::attr(data-ds-steam-deck-compat-handled)").get()
                }
            }

            if app_id is not None and not self.only_games:
                app_url = urlparse(app_href)
                app_url_query_params = parse_qs(app_url.query)

                app_url_query_params["l"] = ["english"]
                app_url_query_params["cc"] = ["us"]

                query_with_locale = urlencode(app_url_query_params, doseq=True)
                app_page_url = urlunparse((
                    app_url.scheme, app_url.netloc,
                    app_url.path, app_url.params,
                    query_with_locale, app_url.fragment
                ))

                yield scrapy.Request(app_page_url, cb_kwargs={ "app_id": app_id }, callback=self.parse_page)
                yield scrapy.Request(f"https://steamcommunity.com/app/{app_id}/reviews/?browsefilter=toprated&snr={snr}&filterLanguage={self.filter_language}&l=english", cb_kwargs={ "app_id": app_id }, callback=self.parse_review)

    def parse_page(self, response, **kwargs):
        page_content = response.css("#game_highlights")

        yield {
            "type": "page",
            "content": {
                "app_id": kwargs.get("app_id"),
                "thumbnail": page_content.css("#gameHeaderImageCtn img.game_header_image_full::attr(src)").get(),
                "description": page_content.css("div.game_description_snippet::text").get(),
                "tags": page_content.css("#glanceCtnResponsiveRight div.popular_tags_ctn div.popular_tags a::text").getall()
            }
        }

    def parse_review(self, response, **kwargs):
        for card in response.css(".apphub_Card"):
            yield {
                "type": "review",
                "content": {
                    "app_id": kwargs.get("app_id"),
                    "found_helpful": card.css("div.found_helpful").get(),
                    "title": card.css(".vote_header .title::text").get(),
                    "hours": card.css(".vote_header .hours::text").get(),
                    "content": card.css(".apphub_CardTextContent").get(),
                    "user_href": card.css(".apphub_CardContentAuthorBlock .apphub_friend_block_container a::attr(href)").get(),
                    "products_in_account": card.css(".apphub_CardContentAuthorBlock .apphub_friend_block_container .apphub_friend_block .apphub_CardContentMoreLink::text").get(),
                    "miniprofile": card.css(".apphub_CardContentAuthorBlock .apphub_friend_block_container .apphub_friend_block::attr(data-miniprofile)").get()
                }
            }

        next_page = response.css("form[id*='MoreContentForm']::attr(action)").get()

        if next_page is not None:
            next_page_url = urlparse(next_page)
            next_page_data = { form_val.css("::attr(name)").get(): form_val.css("::attr(value)").get() for form_val in response.css("form[id*='MoreContentForm'] input") }

            next_page_query = urlencode(next_page_data, doseq=True)
            next_page_url = urlunparse((
                next_page_url.scheme, next_page_url.netloc,
                next_page_url.path, next_page_url.params,
                next_page_query, next_page_url.fragment
            ))

            yield scrapy.Request(next_page_url, cb_kwargs={ "app_id": kwargs.get("app_id") }, callback=self.parse_review)

