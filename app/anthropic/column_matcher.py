import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from anthropic import Anthropic
from anthropic.types.beta import BetaToolParam
from pydantic import BaseModel


class ColumnMapping(BaseModel):
    """Mapping between a CRM column and export column(s)."""

    crm_column: str
    export_column: Optional[str] = None
    confidence: float  # 0.0 to 1.0
    reasoning: str
    is_many_to_one: bool = False
    additional_crm_columns: Optional[List[str]] = None  # For many-to-one mappings


class ColumnMatchingResponse(BaseModel):
    """Response from column matching analysis."""

    mappings: List[ColumnMapping]
    unmapped_crm_columns: List[str]
    unmapped_export_columns: List[str]
    notes: Optional[str] = None


# Column matching prompt
COLUMN_MATCHING_PROMPT = """You are a data integration expert specializing in CRM data mapping. 
Your task is to match CRM columns to their corresponding export columns based on their names and data characteristics.

Consider these matching patterns:
1. **Direct matches**: Same field with different tags (e.g., "email (crm)" → "email (export)")
2. **Semantic matches**: Different names but same meaning (e.g., "phone (crm)" → "direct phone (export)")
3. **Many-to-one mappings**: Multiple CRM fields map to one export field (e.g., multiple LinkedIn fields → "linkedin (export)")
4. **No match**: Some columns may not have corresponding matches

Use the non-null counts and other statistics to validate your matches:
- Similar non-null counts suggest a good match
- Very different counts might indicate different fields
- Consider data patterns and field semantics

Provide confidence scores:
- 1.0: Exact match or very high confidence
- 0.8-0.9: Strong semantic match
- 0.6-0.7: Likely match but needs verification
- Below 0.6: Uncertain, needs human review

CRITICAL: You MUST use the column_matcher tool and provide ALL required fields:
- mappings: array of mapping objects
- unmapped_crm_columns: array of CRM column names with no matches
- unmapped_export_columns: array of export column names with no matches

Even if any of these arrays are empty, you MUST include them in your response."""


# Tool definition for column matching
COLUMN_MATCHING_TOOL: BetaToolParam = {
    "name": "column_matcher",
    "description": "Match CRM columns to export columns based on names and statistics",
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "array",
                "description": "List of column mappings",
                "items": {
                    "type": "object",
                    "properties": {
                        "crm_column": {
                            "type": "string",
                            "description": "The CRM column name",
                        },
                        "export_column": {
                            "type": ["string", "null"],
                            "description": "The matched export column name, or null if no match",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score between 0.0 and 1.0",
                            "minimum": 0.0,
                            "maximum": 1.0,
                        },
                        "reasoning": {
                            "type": "string",
                            "description": "Explanation for the mapping decision",
                        },
                        "is_many_to_one": {
                            "type": "boolean",
                            "description": "Whether this is part of a many-to-one mapping",
                        },
                        "additional_crm_columns": {
                            "type": ["array", "null"],
                            "description": "Other CRM columns that map to the same export column",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["crm_column", "confidence", "reasoning"],
                },
            },
            "unmapped_crm_columns": {
                "type": "array",
                "description": "CRM columns with no export match",
                "items": {"type": "string"},
            },
            "unmapped_export_columns": {
                "type": "array",
                "description": "Export columns with no CRM match",
                "items": {"type": "string"},
            },
            "notes": {
                "type": ["string", "null"],
                "description": "Additional notes about the matching process",
            },
        },
        "required": ["mappings", "unmapped_crm_columns", "unmapped_export_columns"],
    },
}


class ColumnMatcher:
    """Matches CRM columns to export columns using Anthropic's Claude."""

    def __init__(self, model: str = "claude-3-5-sonnet-20241022"):
        """Initialize the matcher with Anthropic API credentials.

        Args:
            model: Model to use (default: claude-3-5-sonnet for better reasoning)
        """
        self._client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model = model

    def prepare_column_stats(
        self, df: pd.DataFrame, columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Prepare statistics for a list of columns.

        Args:
            df: DataFrame containing the data
            columns: List of column names to analyze

        Returns:
            List of dictionaries with column statistics
        """
        stats = []
        for col in columns:
            if col not in df.columns:
                continue

            col_stats = {
                "column_name": col,
                "non_null_count": int(df[col].notna().sum()),
                "null_count": int(df[col].isna().sum()),
                "unique_count": int(df[col].nunique()),
                "data_type": str(df[col].dtype),
            }

            # Add sample values for string columns
            if df[col].dtype == "object":
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    # Get up to 5 sample values
                    sample_values = non_null_values.drop_duplicates().head(5).tolist()
                    col_stats["sample_values"] = [str(v) for v in sample_values]

            stats.append(col_stats)

        return stats

    def match_columns(
        self, df: pd.DataFrame, crm_columns: List[str], export_columns: List[str]
    ) -> ColumnMatchingResponse:
        """Match CRM columns to export columns using LLM.

        Args:
            df: DataFrame containing all the data
            crm_columns: List of CRM column names
            export_columns: List of export column names

        Returns:
            ColumnMatchingResponse with mappings and unmapped columns

        Raises:
            ValueError: If the AI response cannot be parsed properly
        """
        # Prepare statistics for both sets of columns
        crm_stats = self.prepare_column_stats(df, crm_columns)
        export_stats = self.prepare_column_stats(df, export_columns)

        # Create the matching request
        matching_request = {
            "crm_columns": crm_stats,
            "export_columns": export_stats,
            "total_rows": len(df),
        }

        prompt = f"""Match the following CRM columns to their corresponding export columns:

{json.dumps(matching_request, indent=2)}

Analyze the column names and statistics to determine the best matches. Consider:
1. Name similarity (ignoring the (crm) and (export) tags)
2. Non-null counts - similar counts suggest matching fields
3. Data types and sample values
4. Semantic meaning of field names

For phone number fields, note that 'phone (crm)' might map to 'direct phone (export)'.
For LinkedIn fields, multiple CRM fields might map to a single export field.

IMPORTANT: You MUST use the column_matcher tool with ALL required fields:
- mappings: List of all CRM columns and their matches (even if export_column is null)
- unmapped_crm_columns: CRM columns with no export matches (empty array if all match)
- unmapped_export_columns: Export columns with no CRM matches (empty array if all match)"""

        response = self._client.beta.messages.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            system=COLUMN_MATCHING_PROMPT,
            tools=[COLUMN_MATCHING_TOOL],
            tool_choice={"type": "tool", "name": "column_matcher"},
            temperature=0.2,  # Lower temperature for more consistent matching
            max_tokens=4096,
        )

        # Extract matches from response
        result = self._extract_tool_response(response)

        # Check if we got a valid response
        if not result:
            raise ValueError("Failed to extract valid response from AI tool call")

        # Validate required fields are present and provide helpful error messages
        missing_fields = []
        if "mappings" not in result:
            missing_fields.append("mappings")
        if "unmapped_crm_columns" not in result:
            missing_fields.append("unmapped_crm_columns")
        if "unmapped_export_columns" not in result:
            missing_fields.append("unmapped_export_columns")

        if missing_fields:
            raise ValueError(
                f"AI response missing required fields: {missing_fields}. "
                f"Got fields: {list(result.keys())}. "
                f"Full response: {result}"
            )

        # Ensure robust data type handling
        mappings = self._ensure_list(result.get("mappings", []))
        unmapped_crm_columns = self._ensure_list(result.get("unmapped_crm_columns", []))
        unmapped_export_columns = self._ensure_list(
            result.get("unmapped_export_columns", [])
        )

        # Convert dictionary mappings to ColumnMapping objects
        parsed_mappings = []
        parse_errors = []

        for i, mapping in enumerate(mappings):
            try:
                if isinstance(mapping, dict):
                    parsed_mappings.append(ColumnMapping(**mapping))
                elif isinstance(mapping, ColumnMapping):
                    parsed_mappings.append(mapping)
                else:
                    # Handle string case by trying to parse as JSON
                    if isinstance(mapping, str):
                        try:
                            mapping_dict = json.loads(mapping)
                            parsed_mappings.append(ColumnMapping(**mapping_dict))
                        except json.JSONDecodeError as e:
                            parse_errors.append(
                                f"Mapping {i}: Could not parse JSON - {e}"
                            )
                            continue
                    else:
                        parse_errors.append(
                            f"Mapping {i}: Unexpected type {type(mapping)}"
                        )
                        continue
            except Exception as e:
                parse_errors.append(
                    f"Mapping {i}: Failed to create ColumnMapping - {e}"
                )
                continue

        # If we couldn't parse any mappings but we had some, throw an error
        if len(mappings) > 0 and len(parsed_mappings) == 0:
            error_msg = (
                f"Failed to parse any of {len(mappings)} column mappings:\n"
                + "\n".join(parse_errors)
            )
            raise ValueError(error_msg)

        # Log warnings for individual parsing failures
        if parse_errors:
            for error in parse_errors:
                print(f"Warning: {error}")

        return ColumnMatchingResponse(
            mappings=parsed_mappings,
            unmapped_crm_columns=unmapped_crm_columns,
            unmapped_export_columns=unmapped_export_columns,
            notes=result.get("notes"),
        )

    def _extract_tool_response(self, response) -> Dict[str, Any]:
        """Extract tool response from Anthropic message.

        Returns:
            Dict containing the parsed tool response

        Raises:
            ValueError: If no valid tool response is found
        """
        if not hasattr(response, "content") or not response.content:
            raise ValueError("No content found in AI response")

        for content in response.content:
            if hasattr(content, "type") and content.type == "tool_use":
                if not hasattr(content, "input"):
                    raise ValueError("Tool use content has no input")

                input_data = content.input

                # If it's already a dict, return it directly
                if isinstance(input_data, dict):
                    return input_data

                # If it's a string, try to parse it as JSON
                if isinstance(input_data, str):
                    try:
                        parsed_data = json.loads(input_data)
                        if isinstance(parsed_data, dict):
                            return parsed_data
                        else:
                            raise ValueError(
                                f"Parsed JSON is not a dict, got {type(parsed_data)}"
                            )
                    except json.JSONDecodeError as e:
                        # Provide detailed error information
                        raise ValueError(
                            f"Failed to parse tool response as JSON: {e}. "
                            f"Raw response (first 500 chars): {input_data[:500]}"
                        )

                # Handle any other unexpected types
                raise ValueError(f"Unexpected input data type: {type(input_data)}")

        # If we get here, no tool_use content was found
        raise ValueError("No tool_use content found in AI response")

    def _ensure_list(self, data: Any) -> List[Any]:
        """Ensure the data is a list, handling both proper lists and JSON strings."""
        if isinstance(data, list):
            return data
        elif isinstance(data, str):
            try:
                # Try to parse as JSON first
                parsed = json.loads(data)
                if isinstance(parsed, list):
                    return parsed
                else:
                    # If it's not a list after parsing, wrap it
                    return [parsed]
            except json.JSONDecodeError:
                # If it's not valid JSON, treat as a single string item
                return [data]
        elif data is None:
            return []
        else:
            # For any other type, try to convert to list
            try:
                return list(data)
            except (TypeError, ValueError):
                return [data]
