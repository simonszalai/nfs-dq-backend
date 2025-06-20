import re
import warnings
from typing import Dict, Optional
from urllib.parse import urlparse

import pandas as pd
from pydantic import BaseModel, Field

try:
    import phonenumbers  # type: ignore
    from phonenumbers import NumberParseException  # type: ignore
except ImportError:
    phonenumbers = None
    NumberParseException = Exception

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
    ^                                  # start of string
    (?:\+[\d\s\-\.]{1,4})?           # optional country code starting with +
    [\s\-\.]*                         # optional separators
    (?:\(?\d{3}\)?)?                 # optional area code with optional parentheses
    [\s\-\.]*                         # optional separators
    \d{3,4}                           # first part of number (3-4 digits)
    [\s\-\.]*                         # optional separators
    \d{3,4}                           # second part of number (3-4 digits)
    (?:\s*(?:\#|x|ext\.?)\s*\d+)?    # optional extension
    $                                  # end of string
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
    if phonenumbers is None:
        return False
    try:
        parsed = phonenumbers.parse(s, region)
        return phonenumbers.is_valid_number(parsed)
    except NumberParseException:
        return False


def _get_url_format(s: str) -> str:
    """Determine the format signature of a URL."""
    try:
        parsed = urlparse(s if s.startswith(("http://", "https://")) else f"http://{s}")
        components = []
        if parsed.scheme and parsed.scheme in ["http", "https"]:
            components.append(f"scheme:{parsed.scheme}")
        else:
            components.append("no_scheme")
        if parsed.netloc.startswith("www."):
            components.append("www")
        else:
            components.append("no_www")
        if parsed.path and parsed.path != "/" or parsed.query or parsed.fragment:
            components.append("has_path")
        else:
            components.append("no_path")
        return "|".join(components)
    except Exception:
        return "invalid"


def _get_phone_format(s: str) -> str:
    """Determine the format signature of a phone number."""
    components = []

    # Check for country code
    if s.startswith("+"):
        components.append("plus")
        # Check if there's a space after country code
        if len(s) > 1 and s[1:].lstrip() != s[1:]:
            # Find where the country code ends (first non-digit after +)
            i = 1
            while i < len(s) and s[i].isdigit():
                i += 1
            if i < len(s) and s[i] == " ":
                components.append("space_after_country")

    # Check for parentheses
    if "(" in s and ")" in s:
        components.append("parens")

    # Count separators
    num_dashes = s.count("-")
    num_dots = s.count(".")
    num_spaces = len([c for c in s if c == " "])

    # Add separator info
    if num_dashes > 0:
        components.append(f"dash{num_dashes}")
    if num_dots > 0:
        components.append(f"dot{num_dots}")
    if num_spaces > 0:
        components.append(f"space{num_spaces}")

    # If no separators at all
    if num_dashes == 0 and num_dots == 0 and num_spaces == 0:
        components.append("nosep")

    # Check for extensions
    if any(ext in s.lower() for ext in ["#", "x", "ext"]):
        components.append("ext")

    return "|".join(components) if components else "plain"


def _detect_date_format(s: pd.Series) -> Optional[str]:
    """Robustly detect date format using multiple strategies."""
    # Try explicit formats first for better control
    best_format = None
    best_count = 0

    # Test each format to find the best match
    for fmt in DATE_FORMATS:
        try:
            parsed = pd.to_datetime(s, format=fmt, errors="coerce")
            valid_count = parsed.notna().sum()
            if valid_count > best_count:
                best_count = valid_count
                best_format = fmt
        except Exception:
            continue

    # If explicit formats work well, use them
    if best_count / len(s) >= 0.5:
        return best_format

    # Fall back to pandas' automatic parsing with warning suppression
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*Could not infer format.*")
            warnings.filterwarnings(
                "ignore", message=".*The argument 'infer_datetime_format'.*"
            )
            parsed = pd.to_datetime(s, errors="coerce")
            valid_mask = parsed.notna()

        if valid_mask.sum() / len(s) < 0.5:  # Less than 50% parsed
            return None

        # Sample valid dates to determine format characteristics
        sample_dates = s[valid_mask].head(10)

        # Check for consistent separators
        separators = set()
        has_time = False
        has_tz = False

        for date_str in sample_dates:
            if "/" in date_str:
                separators.add("/")
            if "-" in date_str:
                separators.add("-")
            if ":" in date_str:
                has_time = True
            if date_str.endswith("Z") or "+" in date_str[-6:] or "-" in date_str[-6:]:
                has_tz = True

        format_parts = []
        if separators:
            format_parts.append(f"sep:{''.join(sorted(separators))}")
        if has_time:
            format_parts.append("time")
        if has_tz:
            format_parts.append("tz")

        return "|".join(format_parts) if format_parts else "standard"
    except Exception:
        return None


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
    components = []

    # Check for thousands separator
    if "," in s:
        # Check if comma is thousands separator (e.g., 1,234.56)
        if "." in s and s.rindex(".") > s.rindex(","):
            components.append("comma_thousands")
        else:
            # Comma might be decimal separator (e.g., 1.234,56)
            components.append("comma_decimal")
    elif "_" in s:
        components.append("underscore_thousands")

    # Check for decimal separator
    if "." in s:
        if "comma_decimal" not in components:
            components.append("period_decimal")

    # Check for scientific notation
    if "e" in s.lower():
        components.append("scientific")

    return "|".join(components) if components else "plain"


def _get_string_characteristics(s: pd.Series) -> Optional[str]:
    """Determine characteristics of string values in a series."""
    # For strings, format variations might not be meaningful
    # Return None to indicate format_count should be 1
    return None


def _detect_series(
    s: pd.Series, threshold: float
) -> tuple[Optional[str], Optional[int]]:
    """Return detected type and number of unique formats for a column."""
    s = s.dropna().astype(str).str.strip()
    if s.empty:
        return None, None

    total = len(s)

    # Check for URLs
    url_matches = s.str.match(URL_RE.pattern, na=False)
    if (url_matches.sum() / total) >= threshold:
        formats = s[url_matches].apply(_get_url_format).unique()
        return "url", len(formats)

    # Check for emails
    email_matches = s.str.match(EMAIL_RE.pattern, na=False)
    if (email_matches.sum() / total) >= threshold:
        # For emails, format variations are less meaningful
        return "email", None

    # Check for phone numbers with regex first
    # Use direct regex matching instead of pandas str.match for better results
    phone_matches = s.apply(lambda x: PHONE_RE.match(x) is not None)
    if (phone_matches.sum() / total) >= threshold:
        if phonenumbers:
            # Validate with phonenumbers library
            valid_phones = s[phone_matches].apply(_is_valid_phone_number)
            if (valid_phones.sum() / phone_matches.sum()) >= threshold:
                formats = (
                    s[phone_matches & valid_phones].apply(_get_phone_format).unique()
                )
                return "phone", len(formats)
        else:
            # Use basic format detection
            formats = s[phone_matches].apply(_get_phone_format).unique()
            return "phone", len(formats)

    # Check for dates using robust detection
    date_format = _detect_date_format(s)
    if date_format is not None:
        # Try parsing with warning suppression
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*Could not infer format.*")
            warnings.filterwarnings(
                "ignore", message=".*The argument 'infer_datetime_format'.*"
            )
            parsed = pd.to_datetime(s, errors="coerce")
        if (parsed.notna().sum() / total) >= threshold:
            # Count unique date patterns
            sample_formats = (
                s[parsed.notna()]
                .head(100)
                .apply(lambda x: _detect_date_format(pd.Series([x])))
                .unique()
            )
            return "date", len([f for f in sample_formats if f is not None])

    # Check for booleans
    boolean_matches = s.str.lower().isin(BOOLEAN_VALUES.keys())
    if (boolean_matches.sum() / total) >= threshold:
        formats = s[boolean_matches].apply(_get_boolean_format).unique()
        return "boolean", len(formats)

    # Check for integers (before floats)
    try:
        # Remove common thousands separators
        cleaned = s.str.replace(",", "").str.replace("_", "")
        integer_parsed = pd.to_numeric(cleaned, errors="coerce")

        if isinstance(integer_parsed, pd.Series):
            # Check if values are whole numbers
            is_integer = integer_parsed.notna() & (integer_parsed % 1 == 0)
            if (is_integer.sum() / total) >= threshold:
                formats = s[is_integer].apply(_get_integer_format).unique()
                return "integer", len(formats)
    except Exception:
        pass

    # Check for floats
    try:
        # Remove common thousands separators
        cleaned = s.str.replace(",", "").str.replace("_", "")
        float_parsed = pd.to_numeric(cleaned, errors="coerce")

        if isinstance(float_parsed, pd.Series):
            # Check if values have decimal parts
            is_float = float_parsed.notna() & (float_parsed % 1 != 0)
            if (is_float.sum() / total) >= threshold:
                formats = s[is_float].apply(_get_float_format).unique()
                return "float", len(formats)
    except Exception:
        pass

    # Default to string if enough non-null values
    if total > 0:
        # For strings, format count doesn't make much sense
        return "string", None

    return None, None


class ClassifiedColumn(BaseModel):
    type: str
    format_count: int = Field(default=1)


def analyze_inconsistency(
    df: pd.DataFrame, *, threshold: float = 0.8
) -> Dict[str, ClassifiedColumn]:
    """
    Return {column_name: {'type': detected_type, 'format_count': num_formats}} for all columns.
    Columns whose non-null values match URLs, emails, phone numbers, or dates in at least `threshold` proportion
    will have their specific type detected. Others default to 'string' type.
    """
    result: Dict[str, ClassifiedColumn] = {}
    for col in df.columns:
        dtype, format_count = _detect_series(df[col], threshold)
        if dtype is not None:
            result[col] = ClassifiedColumn(
                type=dtype, format_count=format_count if format_count is not None else 1
            )
        else:
            # Default fallback for columns that don't match any specific pattern
            result[col] = ClassifiedColumn(type="string", format_count=1)
    return result
