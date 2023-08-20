import json

import scrapy
from scrapy.selector import Selector


class GamesonlySpider(scrapy.Spider):
    name = "gamesonly"
    allowed_domains = ["store.steampowered.com"]

    def __init__(self, games_number=134664, games_per_page=50, *args, **kwargs):
        super(GamesonlySpider).__init__(*args, **kwargs)

        self.games_number = games_number
        self.games_per_page = games_per_page
        self.pages_number = games_number // games_per_page

    def start_requests(self):
        user_agent = self.settings.attributes.get("USER_AGENT")
        user_agent_val = user_agent.value if user_agent is not None else "None"

        self.logger.info(f"Using 'USER-AGENT' from settings: {user_agent_val}")
        self.logger.info(f"Crawling {self.pages_number} pages ({self.games_per_page} games per page, {self.games_number} games in total)")

        for i in range(self.pages_number):
            yield scrapy.Request(f"https://store.steampowered.com/search/results/?query&start={self.games_per_page * i}&count={self.games_per_page}&dynamic_data=&sort_by=_ASC&snr=1_7_7_230_7&infinite=1&cc=us")

    @staticmethod
    def cast_or_default(val, to_type, default=None):
        try:
            return to_type(val)
        except (ValueError, TypeError):
            return default

    def parse(self, response):
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
                    "tag_ids": [int(x) for x in json.loads(tag_ids)] if tag_ids is not None else [],
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

