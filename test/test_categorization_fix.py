#!/usr/bin/env python3
"""
Test to identify and fix the categorization issue.
"""

import numpy as np
import pandas as pd

from app.enrichment.enrichment_calculation_models import ColumnMappingCalculation
from app.enrichment.enrichment_calculator import EnrichmentStatisticsCalculator

# Create test data that matches the user's scenario
df = pd.DataFrame(
    {
        "email_crm": (
            ["same@email.com"] * 6  # 6 unchanged (same value in both)
            + [None] * 45  # 45 should be added (null->value)
            + ["old1@email.com"] * 28  # 28 should be fixed (different values)
            + [None] * 18  # 18 null->null (this might be the issue)
        ),
        "email_export": (
            ["same@email.com"] * 6  # 6 unchanged
            + ["new@email.com"] * 45  # 45 added
            + ["fixed@email.com"] * 28  # 28 fixed
            + [None] * 18  # 18 null->null
        ),
    }
)

print(f"Total rows: {len(df)}")
print(f"Expected breakdown based on user's report:")
print(f"  Unchanged: 6")
print(f"  Added: 45")
print(f"  Fixed: 28")
print(f"  Discarded: 0")
print(f"  Sum: 79 (but should be 97)")
print(f"  Missing: 18 rows")

# Manually verify the data
manual_counts = {"unchanged": 0, "added": 0, "fixed": 0, "discarded": 0, "null_null": 0}

for i in range(len(df)):
    crm_val = df["email_crm"].iloc[i]
    export_val = df["email_export"].iloc[i]

    crm_is_null = pd.isna(crm_val)
    export_is_null = pd.isna(export_val)

    if crm_is_null and export_is_null:
        manual_counts["null_null"] += 1
    elif crm_is_null and not export_is_null:
        manual_counts["added"] += 1
    elif not crm_is_null and export_is_null:
        manual_counts["discarded"] += 1
    elif not crm_is_null and not export_is_null:
        if str(crm_val) == str(export_val):
            manual_counts["unchanged"] += 1
        else:
            manual_counts["fixed"] += 1

print(f"\nManual verification:")
for key, count in manual_counts.items():
    print(f"  {key}: {count}")
print(
    f"  Total (excluding null_null): {sum(v for k, v in manual_counts.items() if k != 'null_null')}"
)
print(f"  Total (including null_null): {sum(manual_counts.values())}")

# Now test our calculator
calculator = EnrichmentStatisticsCalculator()
column_mapping = ColumnMappingCalculation(
    enrichment_report_id="test",
    crm_column="email_crm",
    export_column="email_export",
    confidence=1.0,
    reasoning="Test",
)

modified_records = set()
try:
    stats = calculator._calculate_column_comparison_stats(
        df, column_mapping, modified_records
    )

    print(f"\nCalculator results:")
    print(f"  Unchanged (good_data): {stats.good_data}")
    print(f"  Added: {stats.added_new_data}")
    print(f"  Fixed: {stats.fixed_data}")
    print(f"  Discarded: {stats.discarded_invalid_data}")
    print(
        f"  Sum: {stats.good_data + stats.added_new_data + stats.fixed_data + stats.discarded_invalid_data}"
    )

    print(f"\nThe issue:")
    print(
        f"  Calculator counts null-null as 'unchanged': {stats.good_data} (includes 18 null-null)"
    )
    print(f"  User expects only value-value matches as 'unchanged': 6")
    print(f"  This is why the numbers don't match user expectations")

except AssertionError as e:
    print(f"\nAssertion error: {e}")

print(
    "\nConclusion: We need to NOT count null-null as 'unchanged' to match user expectations"
)
