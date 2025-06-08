from typing import Dict, Set

import pandas as pd

from app.inconsistency import DATE_FORMATS, ClassifiedColumn, _get_date_format


def count_unique_date_formats(
    classified_columns: Dict[str, ClassifiedColumn], df: pd.DataFrame
) -> int:
    """
    Count the total number of unique date formats across all date-classified columns.

    Args:
        classified_columns: Output from classify_cols, a dictionary
            {column_name: ClassifiedColumn}.
        df: The original DataFrame containing the data.

    Returns:
        int: Number of unique date formats across all date columns.
    """
    # Filter for date columns
    date_columns = [
        col for col, info in classified_columns.items() if info.type == "date"
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
