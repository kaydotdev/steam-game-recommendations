import re
from datetime import datetime

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType

from .common import normalize_date_or_default


def parse_found_helpful_or_default(field_text: str | None, default=0) -> int:
    """Parses 'Found helpful' raw HTML field from the review page.
    Returns a number of users that upvoted a review.

    Args:
        field_text (str | None): Raw HTML text of 'Found helpful'.
        default (int, optional): Return value if input is `None` or failed to parse. Defaults to 0.

    Returns:
        int: Parsed 'Found helpful' value.
    """

    if field_text is None:
        return default

    field_value_occurence = re.findall(r"([\d,]+) people found this review helpful", field_text)

    if len(field_value_occurence) == 0:
        return default

    try:
        return int(field_value_occurence[0].replace(",", ""))
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

    if field_text is None:
        return default

    field_value_occurence = re.findall(r"([\d,]+) people found this review funny", field_text)

    if len(field_value_occurence) == 0:
        return default

    try:
        return int(field_value_occurence[0].replace(",", ""))
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

    if field_text is None:
        return default

    field_value_occurence = re.findall(r"([\d.]+) hrs on record", field_text)

    if len(field_value_occurence) == 0:
        return default

    try:
        return float(field_value_occurence[0])
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

    if field_text is None:
        return default

    field_value_occurence = re.findall(r"([\d,]+) products in account", field_text)

    if len(field_value_occurence) == 0:
        return default

    try:
        return int(field_value_occurence[0].replace(",", ""))
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

    if field_text is None:
        return default

    field_value_occurence = re.findall('(?:<div.*?class=\"date_posted\".*?>)(.*?)(?:<\\/div>)', field_text)

    if len(field_value_occurence) == 0:
        return default

    return normalize_date_or_default(field_value_occurence[0].replace("Posted: ", ""), default=default)


is_recommended_udf = udf(lambda val: val == "Recommended", BooleanType())
found_helpful_udf = udf(lambda val: parse_found_helpful_or_default(val), IntegerType())
found_funny_udf = udf(lambda val: parse_found_funny_or_default(val), IntegerType())
hours_udf = udf(lambda val: parse_hours_or_default(val), DoubleType())
products_udf = udf(lambda val: parse_products_or_default(val), IntegerType())
date_review_udf = udf(lambda val: parse_date_review_or_default(val), DateType())

