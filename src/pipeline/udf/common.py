import re
from datetime import datetime
from typing import Any, List, Pattern, Type

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DateType, IntegerType, StringType


def clean_df(df: DataFrame, columns: list) -> DataFrame:
    """Basic cleaning operation (drop NULLs and duplicates) on selected columns.

    Args:
        df (DataFrame): Raw input dataset.
        columns (list): Columns to process.

    Returns:
        DataFrame: Cleaned dataset.
    """

    return df.dropDuplicates(columns).dropna(subset=tuple(columns))

def cast_or_default(_type: Type, value: Any, default=None) -> Any | None:
    """Converts input `value` to `_type`. If cast fails, returns `default` argument.

    Args:
        _type (Type): Output type.
        value (Any): Input value to cast.
        default (Any | None, optional): Return value if parsing failed. Defaults to None.

    Returns:
        Any | None: Converted type.
    """

    try:
        return _type(value)
    except (ValueError, TypeError):
        return default

def re_first_or_default(pattern: str | Pattern[str], string: str | None, default=None) -> str | None:
    """Return first occurence of all non-overlapping matches in the string.
    If no matches found in `string` returns `default` argument.

    Args:
        pattern (str | Pattern[str]): Regex pattern.
        string (str | None): Input string.
        default (Any, optional): Return value if `string` is `None` or no pattern matches found. Defaults to None.

    Returns:
        str | None: First pattern match.
    """

    if string is None:
        return default

    matches = re.findall(pattern, string)

    return matches[0] if len(matches) > 0 else default

def strip_or_default(val: str | None, default=None) -> str | None:
    """Strips string if value is not `None`.
    Otherwise returns default argument.

    Args:
        val (str | None): Input string to strip.
        default (str | None, optional): Return argument if input is `None`. Defaults to None.

    Returns:
        str | None: Stripped string.
    """

    return val.strip() if val is not None else default

def strip_list_or_default(val: List[str] | None, default=None) -> List[str] | None:
    """Strips each element of a list if value is not `None` and not empty.
    Otherwise returns default argument.

    Args:
        val (List[str] | None): Input list of strings.
        default (List[str] | None, optional): Return argument if input list is `None` or empty. Defaults to None.

    Returns:
        List[str] | None: Stripped list of strings.
    """

    if val is None or len(val) == 0:
        return default

    return [el.strip() for el in val]

def join_strip_list_or_default(val: List[str] | None, default=None) -> str | None:
    """Strips each element of a list and joins elements with `|` character.
    If value is `None` or empty, then returns default argument.

    Args:
        val (List[str] | None): Input list of strings.
        default (str | None, optional): Return argument if input list is `None` or empty. Defaults to None.

    Returns:
        str | None: Stripped and concatenated list of strings.
    """

    stripped_list = strip_list_or_default(val, default=default)

    return "|".join(stripped_list) if stripped_list is not None else default

def normalize_date_or_default(text: str | None, default=None) -> datetime | None:
    """Parses date from string with formats [`%b %d, %Y`, `%b %Y`, `%B %d, %Y`].
    If date parse fails, returns `default`.

    Args:
        text (str | None): Input date in a string format.
        default (datetime | None, optional): Return value if date parsing failed. Defaults to None.

    Returns:
        datetime | None: Parsed datetime.
    """

    if text is None:
        return default

    for fmt in ("%b %d, %Y", "%b %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return default

def format_date_or_default(text: str | None, out_fmt="%Y-%m-%d", default=None) -> str | None:
    """Parses date from string and then converts to the common ISO format.
    If date parse fails, returns `default`. Input acceptable formats are [`%b %d, %Y`, `%b %Y`, `%B %d, %Y`].

    Args:
        text (str | None): Input date in a string format.
        out_fmt (str, optional): Output date ISO format. Defaults to "%Y-%m-%d".
        default (str | None, optional): Return value if date parsing failed. Defaults to None.

    Returns:
        str | None: Parsed and converted datetime.
    """

    parsed_date = normalize_date_or_default(text, default=default)

    return parsed_date.strftime(out_fmt) if parsed_date is not None else default


to_int_udf = udf(lambda val: int(val), IntegerType())
strip_udf = udf(lambda val: strip_or_default(val), StringType())
strip_list_udf = udf(lambda val: strip_list_or_default(val), ArrayType(StringType()))
joined_strip_list_udf = udf(lambda val: join_strip_list_or_default(val), StringType())
normalize_date_udf = udf(lambda val: normalize_date_or_default(val), DateType())
format_date_udf = udf(lambda val: format_date_or_default(val), StringType())

