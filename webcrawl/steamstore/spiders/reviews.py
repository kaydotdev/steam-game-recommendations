import scrapy
import urllib.parse as urltools

from .games import GamesSpider


class ReviewsSpider(GamesSpider):
    name = "reviews"
    allowed_domains = ["store.steampowered.com", "steamcommunity.com"]

    def __init__(
        self, filter_language=None, total_count=None, per_page=None, *args, **kwargs
    ):
        super(ReviewsSpider, self).__init__(*args, **kwargs)

        self.filter_language = filter_language or "all"
        self.total_count = total_count
        self.per_page = per_page or 100

    def parse_game_card(self, response, **kwargs):
        app_id = response.css("*[data-appid]::attr(data-appid)").get()
        snr_mark = kwargs.get("snr", "")

        if app_id:
            self.logger.info(
                f"Parsing the reviews page for game with id={app_id} snr={snr_mark}"
            )

            yield scrapy.Request(
                f"https://steamcommunity.com/app/{app_id}/reviews/?snr={snr_mark}&filterLanguage={self.filter_language}"
                f"&l=english",
                cb_kwargs={"app_id": app_id},
                callback=self.parse_review,
            )
        else:
            self.logger.warning(
                "Failed to detect app ID from the store page. Skipping..."
            )

    def parse_review(self, response, **kwargs):
        app_id = kwargs.get("app_id")

        for card in response.css(".apphub_Card"):
            yield {
                "app_id": app_id,
                "found_helpful": card.css("div.found_helpful").get(),
                "title": card.css(".vote_header .title::text").get(),
                "hours": card.css(".vote_header .hours::text").get(),
                "content": card.css(".apphub_CardTextContent").get(),
                "user_href": card.css(
                    ".apphub_CardContentAuthorBlock .apphub_friend_block_container"
                    " a::attr(href)"
                ).get(),
                "products_in_account": card.css(
                    ".apphub_CardContentAuthorBlock .apphub_friend_block_container"
                    " .apphub_friend_block .apphub_CardContentMoreLink::text"
                ).get(),
                "miniprofile": card.css(
                    ".apphub_CardContentAuthorBlock .apphub_friend_block_container"
                    " .apphub_friend_block::attr(data-miniprofile)"
                ).get(),
            }

        next_page = response.css("form[id*='MoreContentForm']::attr(action)").get()

        if next_page:
            self.logger.debug(
                f"Parsing next review page of the game ID={app_id}: '{next_page}'"
            )

            next_page_url = urltools.urlparse(next_page)
            next_page_data = {
                form_val.css("::attr(name)").get(): form_val.css("::attr(value)").get()
                for form_val in response.css("form[id*='MoreContentForm'] input")
            }

            next_page_query = urltools.urlencode(next_page_data, doseq=True)
            next_page_url = urltools.urlunparse(
                (
                    next_page_url.scheme,
                    next_page_url.netloc,
                    next_page_url.path,
                    next_page_url.params,
                    next_page_query,
                    next_page_url.fragment,
                )
            )

            yield scrapy.Request(
                next_page_url, cb_kwargs=kwargs, callback=self.parse_review
            )
        else:
            self.logger.warning(
                f"Next review page for ID={app_id} not found. Finishing..."
            )
