from datetime import datetime

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType

from .common import normalize_date_or_default, re_first_or_default


def parse_found_helpful_or_default(field_text: str | None, default=0) -> int:
    """Parses 'Found helpful' raw HTML field from the review page.
    Returns a number of users that upvoted a review.

    Args:
        field_text (str | None): Raw HTML text of 'Found helpful'.
        default (int, optional): Return value if input is `None` or failed to parse. Defaults to 0.

    Returns:
        int: Parsed 'Found helpful' value.
    """

    field_match = re_first_or_default(r"([\d,]+) people found this review helpful", field_text, default=str(default))

    try:
        return int(field_match.replace(",", ""))
    except (ValueError, TypeError):
        return default

def parse_found_funny_or_default(field_text: str | None, default=0) -> int:
    """Parses 'Found funny' raw HTML field from the review page.
    Returns a number of users that upvoted a review.

    Args:
        field_text (str | None): Raw HTML text of 'Found funny'.
        default (int, optional): Return value if input is `None` or failed to parse. Defaults to 0.

    Returns:
        int: Parsed 'Found funny' value.
    """

    field_match = re_first_or_default(r"([\d,]+) people found this review funny", field_text, default=str(default))

    try:
        return int(field_match.replace(",", ""))
    except (ValueError, TypeError):
        return default

def parse_hours_or_default(field_text: str | None, default=0.0) -> float:
    """Parses 'Hours played' raw HTML field from the review page.
    Returns hours number in a decimal format.

    Args:
        field_text (str | None): Raw HTML text of 'Hours played'.
        default (float, optional): Return value if input is `None` or failed to parse. Defaults to 0.0.

    Returns:
        float: Parsed 'Hours played' value.
    """

    field_match = re_first_or_default(r"([\d.]+) hrs on record", field_text, default=str(default))

    try:
        return float(field_match)
    except (ValueError, TypeError):
        return default

def parse_products_or_default(field_text: str | None, default=0) -> int:
    """Parses 'Products purchased' raw HTML field from the review page.

    Args:
        field_text (str | None): Raw HTML text of 'Products purchased'.
        default (int, optional): Return value if input is `None` or failed to parse. Defaults to 0.

    Returns:
        int: Parsed 'Products purchased' value.
    """

    field_match = re_first_or_default(r"([\d,]+) products in account", field_text, default=str(default))

    try:
        return int(field_match.replace(",", ""))
    except (ValueError, TypeError):
        return default

def parse_date_review_or_default(field_text: str | None, default=None) -> datetime | None:
    """Parses 'Date posted' raw HTML field from the review page.

    Args:
        field_text (str | None): Raw HTML text of 'Date posted'.
        default (datetime | None, optional): Return value if input is `None` or failed to parse. Defaults to None.

    Returns:
        datetime | None: Parsed 'Date posted' value.
    """

    field_match = re_first_or_default('(?:<div.*?class=\"date_posted\".*?>)(.*?)(?:<\\/div>)', field_text, default=default)

    return normalize_date_or_default(field_match.replace("Posted: ", ""), default=default)


is_recommended_udf = udf(lambda val: val == "Recommended", BooleanType())
found_helpful_udf = udf(lambda val: parse_found_helpful_or_default(val), IntegerType())
found_funny_udf = udf(lambda val: parse_found_funny_or_default(val), IntegerType())
hours_udf = udf(lambda val: parse_hours_or_default(val), DoubleType())
products_udf = udf(lambda val: parse_products_or_default(val), IntegerType())
date_review_udf = udf(lambda val: parse_date_review_or_default(val), DateType())

