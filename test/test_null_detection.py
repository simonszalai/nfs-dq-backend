#!/usr/bin/env python3
"""
Test to check if we're incorrectly identifying values as null/empty.
"""

import pandas as pd

from app.enrichment.enrichment_calculation_models import ColumnMappingCalculation
from app.enrichment.enrichment_calculator import EnrichmentStatisticsCalculator

# Test various edge cases that might be treated as empty
test_values = [
    "test@example.com",
    " test@example.com ",  # with spaces
    "",  # empty string
    " ",  # space only
    "  ",  # multiple spaces
    "\t",  # tab
    "\n",  # newline
    "0",  # zero as string
    0,  # zero as number
    False,  # boolean false
    None,  # actual None
    pd.NA,  # pandas NA
    float("nan"),  # NaN
]

# Create test dataframe
df = pd.DataFrame({"test_crm": test_values, "test_export": test_values})

print("Testing value detection:")
print("=" * 60)

for i, val in enumerate(test_values):
    # Check pandas isna
    is_na = pd.isna(val)

    # Check our logic
    has_value = pd.notna(val) and str(val).strip() != ""

    print(f"Row {i}: value='{val}' (type={type(val).__name__})")
    print(f"  pd.isna(): {is_na}")
    print(f"  pd.notna() and strip() != '': {has_value}")
    print(f"  Would be treated as empty: {not has_value}")
    print()

# Now test with actual email-like data that user might have
print("\nTesting with email data that might be in export:")
print("=" * 60)

email_values = [
    "user@example.com",
    "USER@EXAMPLE.COM",  # uppercase
    "user+tag@example.com",  # with plus
    "user.name@example.com",  # with dot
    "user@subdomain.example.com",  # subdomain
    "noreply@example.com",
    "info@example.com",
    "",  # empty string (might be in CRM)
    None,  # actual None (might be in CRM)
]

df_email = pd.DataFrame(
    {
        "email_crm": [None] * len(email_values),  # All None in CRM
        "email_export": email_values,
    }
)

calculator = EnrichmentStatisticsCalculator()
column_mapping = ColumnMappingCalculation(
    enrichment_report_id="test",
    crm_column="email_crm",
    export_column="email_export",
    confidence=1.0,
    reasoning="Test",
)

modified_records = set()
stats = calculator._calculate_column_comparison_stats(
    df_email, column_mapping, modified_records
)

print(f"Email test results:")
print(f"  Total rows: {len(df_email)}")
print(f"  Added (null CRM -> value export): {stats.added_new_data}")
print(f"  Unchanged (null-null): {stats.good_data}")
print(f"  Export non-null count: {df_email['email_export'].notna().sum()}")

# Check each row
print("\nRow by row analysis:")
for i in range(len(df_email)):
    crm_val = df_email["email_crm"].iloc[i]
    export_val = df_email["email_export"].iloc[i]

    crm_has_value = pd.notna(crm_val) and str(crm_val).strip() != ""
    export_has_value = pd.notna(export_val) and str(export_val).strip() != ""

    print(
        f"Row {i}: CRM={crm_val} (has_value={crm_has_value}), Export='{export_val}' (has_value={export_has_value})"
    )
