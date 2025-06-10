import json
import os
import re
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd
from anthropic import Anthropic
from anthropic.types.beta import BetaToolParam
from pydantic import BaseModel


class WarningType(str, Enum):
    """Types of data quality warnings."""

    INCONSISTENT_FORMAT = "INCONSISTENT_FORMAT"
    DUPLICATE_DATA = "DUPLICATE_DATA"
    DATA_QUALITY = "DATA_QUALITY"
    OTHER = "OTHER"


class Severity(str, Enum):
    """Severity levels for data quality issues."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class DataQualityWarning(BaseModel):
    """Individual data quality warning."""

    type: WarningType
    message: str
    severity: Severity
    affected_count: Optional[int] = None
    examples: Optional[List[str]] = None


class DataQualityResponse(BaseModel):
    """Response from data quality analysis."""

    column_warnings: Dict[str, List[DataQualityWarning]]


# Data quality analysis prompt specifically for HubSpot CRM company data
DATA_QUALITY_PROMPT = """You are a data quality expert analyzing HubSpot CRM company data for go-to-market teams. 
Your task is to identify data quality issues that would impact sales, marketing, and revenue operations.

You're analyzing company records with fields like company name, website, industry, revenue, employee count, location, etc.

Focus on identifying:
1. **Data Consistency Issues**:
   - Inconsistent company name formats (Inc. vs Incorporated, LLC vs L.L.C.)
   - Mixed date formats
   - Inconsistent industry classifications
   - Mixed currency formats in revenue fields

2. **Data Standardization Problems**:
   - Multiple representations of same company (Microsoft vs Microsoft Corp vs MSFT)
   - Country names vs codes (United States vs US vs USA)
   - Industry variations (SaaS vs Software as a Service)

3. **Suspicious Patterns**:
   - Test data (test company, example.com, 123 Main St)
   - Placeholder values (TBD, N/A in wrong fields, XXX)
   - Default HubSpot values that were never updated
   - Rare values that appear only once or twice (potential typos)

4. **Invalid Data**:
   - Invalid websites (missing protocols, typos like .con instead of .com)
   - Future founding dates
   - Impossible employee counts or revenue figures
   - Invalid email domains

5. **Business Logic Violations**:
   - Revenue without employee count
   - Subsidiary with higher revenue than parent
   - Conflicting industry and description

Pay special attention to:
- The most common values (potential standardization opportunities)
- The least common values (potential errors, typos, or outliers)

DO NOT report on missing values or empty fields - these are handled separately.

For each issue, provide specific examples and assess business impact:
- CRITICAL: Will break integrations, prevent outreach, or cause major business issues
- HIGH: Significant impact on segmentation, personalization, or analytics
- MEDIUM: Noticeable issues affecting data reliability
- LOW: Minor inconsistencies

Provide actionable warnings that demonstrate the value of data cleaning services."""

# Tool definition for data quality analysis
DATA_QUALITY_TOOL: BetaToolParam = {
    "name": "data_quality_analyzer",
    "description": "Analyze HubSpot CRM company data for quality issues and return warnings",
    "input_schema": {
        "type": "object",
        "properties": {
            "column_warnings": {
                "type": "object",
                "description": "Dictionary mapping column names to their warnings",
                "additionalProperties": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [
                                    "INCONSISTENT_FORMAT",
                                    "DUPLICATE_DATA",
                                    "DATA_QUALITY",
                                    "OTHER",
                                ],
                                "description": "Type of warning",
                            },
                            "message": {
                                "type": "string",
                                "description": "Specific, actionable description of the issue",
                            },
                            "severity": {
                                "type": "string",
                                "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                                "description": "Business impact severity",
                            },
                            "affected_count": {
                                "type": "integer",
                                "description": "Number of records affected by this issue",
                            },
                            "examples": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "2-3 example values showing the issue",
                                "maxItems": 3,
                            },
                        },
                        "required": ["type", "message", "severity"],
                    },
                },
            }
        },
        "required": ["column_warnings"],
    },
}


class DataQualityAnalyzer:
    """Analyzes HubSpot CRM data quality using Anthropic's Claude."""

    def __init__(self, model: str = "claude-sonnet-4-20250514"):
        """Initialize the analyzer with Anthropic API credentials.

        Args:
            api_key: Anthropic API key
            model: Model to use (default: claude-3-haiku for cost efficiency)
        """
        self._client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model = model

    def prepare_column_analysis(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """Prepare comprehensive analysis data for a single column.

        Args:
            df: The dataframe containing the data
            column: Column name to analyze

        Returns:
            Dictionary with column statistics and analysis
        """
        col_data = df[column].dropna()  # We handle missing values separately

        analysis = {
            "column_name": column,
            "total_records": len(df),
            "non_null_records": len(col_data),
            "unique_count": col_data.nunique(),
            "data_type": str(col_data.dtype),
        }

        # Get all value counts
        all_value_counts = col_data.value_counts()

        # Top 15 most common values
        top_15 = all_value_counts.head(15).to_dict()
        analysis["most_common_values"] = {str(k): v for k, v in top_15.items()}

        # Bottom 15 least common values (excluding single occurrences if too many)
        if len(all_value_counts) > 15:
            # Get values that appear least frequently
            bottom_15 = all_value_counts.tail(15).to_dict()
            analysis["least_common_values"] = {str(k): v for k, v in bottom_15.items()}

            # Also note how many values appear only once
            single_occurrence_count = (all_value_counts == 1).sum()
            analysis["single_occurrence_count"] = int(single_occurrence_count)
        else:
            analysis["least_common_values"] = {
                str(k): v for k, v in all_value_counts.to_dict().items()
            }
            analysis["single_occurrence_count"] = 0

        # For string columns, add pattern analysis on ALL data
        if col_data.dtype == "object" and len(col_data) > 0:
            patterns = self._analyze_patterns(col_data)
            analysis["patterns"] = patterns

            # Add length statistics for strings
            lengths = col_data.astype(str).str.len()
            analysis["length_stats"] = {
                "min": int(lengths.min()),
                "max": int(lengths.max()),
                "mean": float(lengths.mean()),
                "std": float(lengths.std()) if len(lengths) > 1 else 0,
            }

        # For numeric columns, add range statistics
        if pd.api.types.is_numeric_dtype(col_data):
            analysis["numeric_stats"] = {
                "min": float(col_data.min()),
                "max": float(col_data.max()),
                "mean": float(col_data.mean()),
                "std": float(col_data.std()) if len(col_data) > 1 else 0,
                "has_negative": bool((col_data < 0).any()),
                "has_zero": bool((col_data == 0).any()),
            }

        return analysis

    def _analyze_patterns(self, series: pd.Series) -> Dict[str, int]:
        """Analyze common patterns in string data across ALL rows."""
        patterns = {
            "contains_email": 0,
            "contains_url": 0,
            "all_caps": 0,
            "all_lower": 0,
            "mixed_case": 0,
            "contains_numbers": 0,
            "special_chars": 0,
            "starts_with_number": 0,
            "ends_with_punctuation": 0,
            "contains_test_pattern": 0,
        }

        email_pattern = r"@[\w\.-]+"
        url_pattern = r"https?://|www\."
        test_pattern = r"test|example|demo|sample|dummy"

        # Analyze ALL rows
        for value in series:
            if pd.isna(value):
                continue
            str_val = str(value)

            if re.search(email_pattern, str_val):
                patterns["contains_email"] += 1
            if re.search(url_pattern, str_val):
                patterns["contains_url"] += 1
            if str_val.isupper() and len(str_val) > 1:
                patterns["all_caps"] += 1
            elif str_val.islower() and len(str_val) > 1:
                patterns["all_lower"] += 1
            elif len(str_val) > 1:
                patterns["mixed_case"] += 1
            if any(c.isdigit() for c in str_val):
                patterns["contains_numbers"] += 1
            if re.search(r"[^a-zA-Z0-9\s]", str_val):
                patterns["special_chars"] += 1
            if str_val and str_val[0].isdigit():
                patterns["starts_with_number"] += 1
            if str_val and str_val[-1] in ".,;:!?":
                patterns["ends_with_punctuation"] += 1
            if re.search(test_pattern, str_val, re.IGNORECASE):
                patterns["contains_test_pattern"] += 1

        return patterns

    def _extract_tool_response(self, response) -> Dict[str, Any]:
        """Extract tool response from Anthropic message."""
        for content in response.content:
            if hasattr(content, "type") and content.type == "tool_use":
                return content.input
        return {}

    def analyze_dataframe(
        self, df: pd.DataFrame, columns: Optional[List[str]] = None
    ) -> DataQualityResponse:
        """Analyze data quality issues across all or specified columns.

        Args:
            df: DataFrame to analyze
            columns: Specific columns to analyze (None = all columns)

        Returns:
            DataQualityResponse object with column warnings
        """
        if columns is None:
            columns = df.columns.tolist()

        # Prepare analysis data for all columns
        columns_data = []
        for column in columns:
            col_analysis = self.prepare_column_analysis(df, column)
            columns_data.append(col_analysis)

        # Create the analysis request
        analysis_request = {
            "dataset_info": {
                "total_rows": len(df),
                "total_columns": len(columns),
                "source": "HubSpot CRM",
                "entity_type": "companies",
            },
            "columns": columns_data,
        }

        # Send to Claude for analysis
        prompt = f"""Analyze the following HubSpot CRM company data for quality issues:

{json.dumps(analysis_request, indent=2)}

For each column with issues, identify specific data quality problems. Pay special attention to:
1. The most common values - look for standardization opportunities
2. The least common values - these often contain typos, test data, or errors
3. Patterns that indicate data quality issues

Focus on actionable issues that would impact go-to-market operations."""

        response = self._client.beta.messages.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            system=DATA_QUALITY_PROMPT,
            tools=[DATA_QUALITY_TOOL],
            tool_choice={"type": "tool", "name": "data_quality_analyzer"},
            temperature=0.3,
            max_tokens=4096,
        )

        # Extract warnings from response
        result = self._extract_tool_response(response)
        return DataQualityResponse(column_warnings=result.get("column_warnings", {}))
