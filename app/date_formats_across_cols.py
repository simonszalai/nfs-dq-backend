from typing import Dict, Set

import pandas as pd

from app.detect_inconsistent_cols import DATE_FORMATS, _get_date_format


def count_unique_date_formats(
    classification: Dict[str, Dict[str, str | int]], df: pd.DataFrame
) -> int:
    """
    Count the total number of unique date formats across all date-classified columns.

    Args:
        classification: Output from classify_columns_by_regex, a nested dictionary
            {column_name: {"type": str, "format_count": int}}.
        df: The original DataFrame containing the data.

    Returns:
        int: Number of unique date formats across all date columns.
    """
    # Filter for date columns
    date_columns = [
        col for col, info in classification.items() if info["type"] == "date"
    ]

    if not date_columns:
        return 0

    # Collect unique formats across all date columns
    unique_formats: Set[str] = set()

    for col in date_columns:
        # Process non-null values
        s = df[col].dropna().astype(str).str.strip()
        if s.empty:
            continue
        # Get formats for valid dates
        formats = s.apply(lambda x: _get_date_format(x, DATE_FORMATS))
        # Exclude unknown formats and add to set
        valid_formats = {fmt for fmt in formats if fmt != "unknown"}
        unique_formats.update(valid_formats)

    return len(unique_formats)
