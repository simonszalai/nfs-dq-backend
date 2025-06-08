import re
from typing import Dict
from urllib.parse import urlparse

import pandas as pd
import phonenumbers
from phonenumbers import NumberParseException

# ── constants ────────────────────────────────────────────────────────────────
BOOLEAN_VALUES = {
    "true": True,
    "false": False,
    "yes": True,
    "no": False,
    "y": True,
    "n": False,
    "1": True,
    "0": False,
    "on": True,
    "off": False,
    "t": True,
    "f": False,
}

# ── regexes ────────────────────────────────────────────────────────────────
URL_RE = re.compile(
    r"""
    ^(?:https?://)?                    # optional scheme
    (?:www\.)?                         # optional www
    [a-zA-Z0-9][a-zA-Z0-9\-]*         # domain start
    (?:\.[a-zA-Z0-9\-]+)*             # subdomains
    \.[a-zA-Z]{2,}                     # TLD
    (?:/[\w\-\.~%!*'();:@&=+$,/?#]*)?$ # optional path/query
    """,
    re.IGNORECASE | re.VERBOSE,
)

EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    re.IGNORECASE,
)

PHONE_RE = re.compile(
    r"""
    ^(?:\+?\d{1,3})?                  # optional country code
    (?:[\-\s(]*\d{3}[\-\s)]*)         # area code (required)
    \d{3}[\-\s]?\d{4}                 # main number (7 digits)
    (?:\s*(?:\#|x|ext\.?)\s*\d+)?$    # optional extension
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Expanded list of date formats to handle various representations
DATE_FORMATS = [
    # ISO-like formats
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
    # US formats
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m-%d-%Y",
    "%m-%d-%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    # European formats
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d-%m-%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    # Other common formats
    "%Y/%m/%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    # Short formats
    "%Y%m%d",
    "%m%d%Y",
    "%d%m%Y",
    # With month names
    "%Y %b %d",
    "%Y %B %d",
    "%d %b %Y",
    "%d %B %Y",
]


def _is_valid_phone_number(s: str, region: str = "US") -> bool:
    """Check if a string is a valid phone number using phonenumbers library."""
    try:
        parsed = phonenumbers.parse(s, region)
        return phonenumbers.is_valid_number(parsed)
    except NumberParseException:
        return False


def _get_url_format(s: str) -> str:
    """Determine the format signature of a URL."""
    parsed = urlparse(s if s.startswith("http") else f"http://{s}")
    components = []
    if parsed.scheme:
        components.append(f"scheme:{parsed.scheme}")
    else:
        components.append("no_scheme")
    if parsed.netloc.startswith("www."):
        components.append("www")
    else:
        components.append("no_www")
    if parsed.path or parsed.query or parsed.fragment:
        components.append("has_path")
    else:
        components.append("no_path")
    return "|".join(components)


def _get_phone_format(s: str) -> str:
    """Determine the format signature of a phone number."""
    try:
        parsed = phonenumbers.parse(s, "US")
        # Use raw string to capture formatting
        components = []
        if s.startswith("+"):
            components.append("has_country")
        else:
            components.append("no_country")
        if "(" in s and ")" in s:
            components.append("area_parens")
        else:
            components.append("no_parens")
        separators = set(s) & {"-", ".", " "}
        components.append(f"sep:{separators if separators else 'none'}")
        if any(ext in s.lower() for ext in ["#", "x", "ext"]):
            components.append("has_ext")
        else:
            components.append("no_ext")
        return "|".join(components)
    except NumberParseException:
        return "invalid"


def _get_date_format(s: str, formats: list) -> str:
    """Determine the format of a date string."""
    for fmt in formats:
        try:
            pd.to_datetime(s, format=fmt, errors="raise")
            # Check for timezone
            if s.endswith("Z") or "+" in s or "-" in s[-6:]:
                return f"{fmt}|tz"
            return fmt
        except ValueError:
            continue
    return "unknown"


def _get_boolean_format(s: str) -> str:
    """Determine the format of a boolean value."""
    s_lower = s.lower()
    if s_lower in ["true", "false"]:
        return "true_false"
    elif s_lower in ["yes", "no"]:
        return "yes_no"
    elif s_lower in ["y", "n"]:
        return "y_n"
    elif s_lower in ["1", "0"]:
        return "1_0"
    elif s_lower in ["on", "off"]:
        return "on_off"
    elif s_lower in ["t", "f"]:
        return "t_f"
    return "unknown"


def _get_integer_format(s: str) -> str:
    """Determine the format of an integer value."""
    if "," in s:
        return "comma_separated"
    elif "_" in s:
        return "underscore_separated"
    return "plain"


def _get_float_format(s: str) -> str:
    """Determine the format of a float value."""
    if "," in s:
        return "comma_separated"
    elif "_" in s:
        return "underscore_separated"
    elif "." in s:
        return "decimal_point"
    return "plain"


def _get_string_format(s: pd.Series) -> pd.Series:
    """Determine the format characteristics of string values."""
    return s.apply(
        lambda x: f"case:{'upper' if x.isupper() else 'lower' if x.islower() else 'mixed'}"
    )


def _detect_series(s: pd.Series, threshold: float) -> tuple[str | None, int]:
    """Return detected type and number of unique formats for a column."""
    s = s.dropna().astype(str).str.strip()
    if s.empty:
        return None, 0

    total = len(s)

    # Check for URLs
    url_matches = s.str.match(URL_RE)
    if (url_matches.sum() / total) >= threshold:
        formats = s[url_matches].apply(_get_url_format).unique()
        return "url", len(formats)

    # Check for emails
    email_matches = s.str.match(EMAIL_RE)
    if (email_matches.sum() / total) >= threshold:
        # For emails, count case variations or domain structure
        formats = (
            s[email_matches]
            .apply(
                lambda x: f"{'upper' if x.isupper() else 'lower'}|{len(x.split('.')) - 1}"
            )
            .unique()
        )
        return "email", len(formats)

    # Check for phone numbers with regex first
    phone_matches = s.str.match(PHONE_RE)
    if (phone_matches.sum() / total) >= threshold:
        # Validate with phonenumbers library
        valid_phones = s[phone_matches].apply(_is_valid_phone_number)
        if (valid_phones.sum() / phone_matches.sum()) >= threshold:
            formats = s[phone_matches & valid_phones].apply(_get_phone_format).unique()
            return "phone", len(formats)
        return None, 0

    # Check for dates using multiple formats
    for fmt in DATE_FORMATS:
        parsed = pd.to_datetime(s, format=fmt, errors="coerce", utc=False)
        if (parsed.notna().sum() / total) >= threshold:
            formats = (
                s[parsed.notna()]
                .apply(lambda x: _get_date_format(x, DATE_FORMATS))
                .unique()
            )
            return "date", len(formats)

    # Check for booleans
    boolean_matches = s.str.lower().isin(BOOLEAN_VALUES.keys())
    if (boolean_matches.sum() / total) >= threshold:
        formats = s[boolean_matches].apply(_get_boolean_format).unique()
        return "boolean", len(formats)

    # Check for integers (before floats)
    try:
        # Remove commas for integer parsing
        cleaned = s.str.replace(",", "")
        integer_parsed = pd.to_numeric(cleaned, errors="coerce")
        is_integer = integer_parsed.notna() & (
            integer_parsed == integer_parsed.astype("Int64", errors="ignore")
        )
        if (is_integer.sum() / total) >= threshold:
            formats = s[is_integer].apply(_get_integer_format).unique()
            return "integer", len(formats)
    except:
        pass

    # Check for floats
    try:
        float_parsed = pd.to_numeric(s.str.replace(",", ""), errors="coerce")
        is_float = float_parsed.notna() & ~(
            float_parsed == float_parsed.astype("Int64", errors="ignore")
        )
        if (is_float.sum() / total) >= threshold:
            formats = s[is_float].apply(_get_float_format).unique()
            return "float", len(formats)
    except:
        pass

    # Default to string if enough non-null values
    if total > 0:
        formats = _get_string_format(s).unique()
        return "string", len(formats)

    return None, 0


def classify_cols(
    df: pd.DataFrame, *, threshold: float = 0.8
) -> Dict[str, Dict[str, str | int]]:
    """
    Return {column_name: {'type': detected_type, 'format_count': num_formats}} for columns
    whose non-null values match URLs, emails, phone numbers, or dates in at least `threshold` proportion.
    """
    result: Dict[str, Dict[str, str | int]] = {}
    for col in df.columns:
        dtype, format_count = _detect_series(df[col], threshold)
        if dtype is not None:
            result[col] = {"type": dtype, "format_count": format_count}
    return result


def detect_inconsistent_cols(
    df: pd.DataFrame, *, threshold: float = 0.8
) -> Dict[str, Dict[str, str | int]]:
    classified_cols = classify_cols(df, threshold=threshold)

    result: Dict[str, Dict[str, str | int]] = {}
    for col in classified_cols:
        if classified_cols[col]["format_count"] > 1:
            result[col] = classified_cols[col]

    return result
