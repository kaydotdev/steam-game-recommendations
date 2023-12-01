import math
import re

from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
)

from .common import cast_or_default, re_first_or_default


def parse_title_or_default(title: str | None, default=None) -> str | None:
    """Parses game title raw HTML field from the game page profile.

    Args:
        title (str | None): Raw HTML title.
        default (str | None, optional): Return value if input is `None`. Defaults to None.

    Returns:
        str | None: Parsed game title value.
    """

    return title.strip().replace(",", "") if title is not None else default


def parse_rating_or_default(rating: str | None, default=None) -> str | None:
    """Parses 'Rating' raw HTML field from the game page profile.

    Args:
        rating (str | None): Raw HTML text of 'Rating'.
        default (str | None, optional): Return value if input is `None`. Defaults to None.

    Returns:
        str | None: Parsed 'Rating'.
    """

    return rating.split("<br>")[0].strip() if rating is not None else default


def parse_positive_ratio_or_default(
    positive_ratio: str | None, default=None
) -> int | None:
    """Parses 'Positive reviews ratio' raw HTML field from the game page profile.
    Returns the percentage value as an `integer`.

    Args:
        positive_ratio (str | None): Raw HTML text of 'Positive reviews ratio'.
        default (int | None, optional): Return value if input is `None` or failed to parse. Defaults to None.

    Returns:
        int | None: Parsed 'Positive reviews ratio' value.
    """

    field_match = re_first_or_default(r"<br>(\d+)\%", positive_ratio, default=default)

    return cast_or_default(int, field_match.strip(), default=default)


def parse_reviews_number_or_default(
    reviews_number: str | None, default=None
) -> int | None:
    """Parses 'User reviews' raw HTML field from the game page profile.

    Args:
        reviews_number (str | None): Raw HTML text of 'User reviews'.
        default (int | None, optional): Return value if input is `None` or failed to parse. Defaults to None.

    Returns:
        int | None: Parsed 'User reviews' value.
    """

    field_match = re_first_or_default(
        r"the ([\d,]+) user", reviews_number, default=default
    )

    return cast_or_default(int, field_match.strip().replace(",", ""), default=default)


def parse_pricing_or_default(pricing: str | None, default=None) -> float | None:
    """Parses 'Pricing' raw HTML field from the game page profile. Price value is displayed in `$`.

    Args:
        pricing (str | None): Raw HTML text of 'Pricing'.
        default (float | None, optional): Return value if input is `None` or failed to parse. Defaults to None.

    Returns:
        float | None: Parsed 'Pricing' value.
    """

    try:
        return float(pricing) / 100.0
    except (ValueError, TypeError):
        return default


def parse_original_price_or_default(original_price: str | None, default=0.0) -> float:
    """Parses 'Original price' raw HTML field from the game page profile. Price value is displayed in `$`.

    Args:
        original_price (str | None): Raw HTML text of 'Original price'.
        default (float, optional): Return value if input is `None` or failed to parse. Defaults to 0.0.

    Returns:
        float: Parsed 'Original price' value.
    """

    if original_price is None:
        return default

    parsed_price = (
        original_price.replace(
            '<div class="col search_price responsive_secondrow">', ""
        )
        .replace('<div class="col search_price  responsive_secondrow">', "")
        .replace('<div class="col search_price discounted responsive_secondrow">', "")
        .replace("</div>", "")
        .strip()
    )

    if "Free" in parsed_price:
        return default
    elif "color: #888888;" in parsed_price:
        extracted_price = re.findall(r"<strike>\$([\S]+)</strike>", parsed_price)[0]

        return cast_or_default(float, extracted_price.strip(), default=default)

    return cast_or_default(float, parsed_price.replace("$", ""), default=default)


def calculate_discount(args: list) -> float | None:
    """Calculates discount from aggregated columns of `final` and `original` game price.

    Args:
        args (list): A list of column values: [`final`, `original`].

    Returns:
        float | None: Calculated discount. Returns `None` if atleast one column value is `None`.
    """

    final, original = args[0], args[1]

    if final is None or original is None:
        return None
    elif math.isclose(original, 0.0):
        return 0
    else:
        return int((original - final) / original * 100)


is_win_udf = udf(lambda val: "win" in val, BooleanType())
is_mac_udf = udf(lambda val: "mac" in val, BooleanType())
is_linux_udf = udf(lambda val: "linux" in val, BooleanType())

strip_title_udf = udf(lambda val: parse_title_or_default(val), StringType())
rating_udf = udf(lambda val: parse_rating_or_default(val), StringType())
positive_ratio_udf = udf(
    lambda val: parse_positive_ratio_or_default(val), IntegerType()
)
reviews_number_udf = udf(
    lambda val: parse_reviews_number_or_default(val), IntegerType()
)
pricing_udf = udf(lambda val: parse_pricing_or_default(val), DoubleType())
original_price_udf = udf(lambda val: parse_original_price_or_default(val), DoubleType())
steam_deck_udf = udf(lambda val: val == "true", BooleanType())
calculate_discount_udf = udf(lambda arr: calculate_discount(arr), IntegerType())

replace_escape_chr_udf = udf(lambda x: x.replace('"', "'"), StringType())
description_udf = udf(lambda x: x if x is not None else "", StringType())
deserialize_tags_udf = udf(
    lambda x: x.split("|") if x is not None else [], ArrayType(StringType())
)
