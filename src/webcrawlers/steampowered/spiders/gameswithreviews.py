import json
from urllib.parse import urlencode

import scrapy
from scrapy.selector import Selector


class GameswithReviewsSpider(scrapy.Spider):
    name = "gameswithreviews"
    allowed_domains = ["store.steampowered.com", "steamcommunity.com"]

    def __init__(self, games_number=134664, games_per_page=50, *args, **kwargs):
        super(GameswithReviewsSpider).__init__(*args, **kwargs)

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

            if app_id is not None:
                yield scrapy.Request(f"{app_href}?l=english&cc=us", cb_kwargs={ "app_id": app_id }, callback=self.parse_page)
                yield scrapy.Request(f"https://steamcommunity.com/app/{app_id}/reviews/?browsefilter=toprated&snr=1_5_100010_&filterLanguage=english&l=english", cb_kwargs={ "app_id": app_id }, callback=self.parse_review)

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
                "record": "review",
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
            next_page_data = { form_val.css("::attr(name)").get(): form_val.css("::attr(value)").get() for form_val in response.css("form[id*='MoreContentForm'] input") }
            next_page_url = next_page + "?" + urlencode(next_page_data)

            yield scrapy.Request(next_page_url, cb_kwargs={ "app_id": kwargs.get("app_id") }, callback=self.parse_review)

