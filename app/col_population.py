import pandas as pd


def analyze_column_population(df: pd.DataFrame) -> list:
    """
    Analyzes column population in a DataFrame and returns population counts for all columns.

    Args:
        df: Input DataFrame

    Returns:
        List of dictionaries containing column names and their population counts
    """

    def is_filled(val):
        if pd.isnull(val):
            return False
        if isinstance(val, str) and val.strip() == "":
            return False
        return True

    fill_counts = df.apply(lambda col: col.apply(is_filled).sum())
    return [
        {"column_name": col, "populated_count": int(count)}
        for col, count in fill_counts.items()
    ]
