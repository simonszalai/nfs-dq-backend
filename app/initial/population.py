from typing import Dict

import pandas as pd
from pydantic import BaseModel


class ColumnPopulation(BaseModel):
    populated_count: int


def analyze_population(df: pd.DataFrame) -> Dict[str, ColumnPopulation]:
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
    return {
        col: ColumnPopulation(populated_count=int(fill_counts[col]))
        for col in fill_counts.index
    }
