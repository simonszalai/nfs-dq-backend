#!/usr/bin/env python3
"""
Test to simulate what might be happening with the actual data.
If user says export has no nulls but we're counting null-null cases,
then maybe the export has empty strings or other edge cases.
"""

import pandas as pd

from app.enrichment.enrichment_calculation_models import ColumnMappingCalculation
from app.enrichment.enrichment_calculator import EnrichmentStatisticsCalculator

# Simulate possible scenarios
scenarios = [
    {
        "name": "Export has empty strings (not nulls)",
        "data": pd.DataFrame(
            {
                "email_crm": ["same@email.com"] * 6
                + [None] * 45
                + ["old@email.com"] * 28
                + [None] * 18,
                "email_export": ["same@email.com"] * 6
                + ["new@email.com"] * 45
                + ["fixed@email.com"] * 28
                + [""] * 18,  # Empty strings, not None!
            }
        ),
    },
    {
        "name": "Export has spaces/whitespace",
        "data": pd.DataFrame(
            {
                "email_crm": ["same@email.com"] * 6
                + [None] * 45
                + ["old@email.com"] * 28
                + [None] * 18,
                "email_export": ["same@email.com"] * 6
                + ["new@email.com"] * 45
                + ["fixed@email.com"] * 28
                + [" "] * 18,  # Spaces
            }
        ),
    },
    {
        "name": "Export has 'None' string (not None value)",
        "data": pd.DataFrame(
            {
                "email_crm": ["same@email.com"] * 6
                + [None] * 45
                + ["old@email.com"] * 28
                + [None] * 18,
                "email_export": ["same@email.com"] * 6
                + ["new@email.com"] * 45
                + ["fixed@email.com"] * 28
                + ["None"] * 18,  # String 'None'
            }
        ),
    },
    {
        "name": "Export actually has valid emails everywhere",
        "data": pd.DataFrame(
            {
                "email_crm": ["same@email.com"] * 6
                + [None] * 45
                + ["old@email.com"] * 28
                + [""] * 18,  # Empty strings in CRM
                "email_export": ["same@email.com"] * 6
                + ["new@email.com"] * 45
                + ["fixed@email.com"] * 28
                + ["default@email.com"] * 18,  # All valid
            }
        ),
    },
]

calculator = EnrichmentStatisticsCalculator()

for scenario in scenarios:
    print(f"\n{'=' * 70}")
    print(f"Scenario: {scenario['name']}")
    print("=" * 70)

    df = scenario["data"]

    # Check what pandas thinks about nulls
    print(f"Data analysis:")
    print(f"  Total rows: {len(df)}")
    print(f"  CRM nulls (pd.isna): {df['email_crm'].isna().sum()}")
    print(f"  Export nulls (pd.isna): {df['email_export'].isna().sum()}")
    print(f"  CRM empty strings: {(df['email_crm'] == '').sum()}")
    print(f"  Export empty strings: {(df['email_export'] == '').sum()}")

    # Check export values
    print(f"\nExport column unique values:")
    unique_vals = df["email_export"].value_counts(dropna=False)
    for val, count in unique_vals.items():
        print(f"  '{val}' (type={type(val).__name__}): {count}")

    column_mapping = ColumnMappingCalculation(
        enrichment_report_id="test",
        crm_column="email_crm",
        export_column="email_export",
        confidence=1.0,
        reasoning="Test",
    )

    modified_records = set()
    stats = calculator._calculate_column_comparison_stats(
        df, column_mapping, modified_records
    )

    print(f"\nCalculator results:")
    print(f"  Unchanged: {stats.good_data}")
    print(f"  Added: {stats.added_new_data}")
    print(f"  Fixed: {stats.fixed_data}")
    print(f"  Discarded: {stats.discarded_invalid_data}")
    print(
        f"  Sum: {stats.good_data + stats.added_new_data + stats.fixed_data + stats.discarded_invalid_data}"
    )

    print(f"\nPercentage analysis:")
    print(f"  Correct values after: {stats.correct_values_after}")
    print(f"  Non-null export (pd.notna): {df['email_export'].notna().sum()}")
    print(f"  Correct % after: {stats.correct_percentage_after:.1f}%")

    # If this matches user's scenario
    if stats.good_data == 6 and stats.added_new_data == 45 and stats.fixed_data == 28:
        print("\n⚠️  This scenario DOES NOT match user's report (sum = 79, not 97)")
    elif (
        stats.good_data == 24 and stats.added_new_data == 45 and stats.fixed_data == 28
    ):
        print("\n✅ This scenario MATCHES what the calculator produces!")
        print("   The 18 'extra' unchanged are null-null (or empty-empty) cases")
