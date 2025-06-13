from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class EnrichmentReportCalculation(BaseModel):
    """Data model for enrichment calculations (non-database)"""

    id: str = Field(default_factory=lambda: str(uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Basic run information
    total_rows: int
    total_crm_columns: int
    total_export_columns: int

    # Global statistics
    new_columns_count: int
    many_to_one_count: int  # Number of export columns with many-to-one relationships
    columns_reduced_by_merging: int  # Total CRM columns reduced by merging
    records_modified_count: int  # Number of records with any modification
    export_columns_created: int  # Total export columns created

    # Related data
    column_mappings: List["ColumnMappingCalculation"] = Field(default_factory=list)


class ColumnMappingCalculation(BaseModel):
    """Data model for column mapping calculations (non-database)"""

    id: str = Field(default_factory=lambda: str(uuid4()))
    enrichment_report_id: str

    # Column names
    crm_column: str
    export_column: Optional[str] = None

    # Mapping details
    is_many_to_one: bool = False
    additional_crm_columns: Optional[List[str]] = None
    confidence: float
    reasoning: str

    # Related data
    comparison_stats: Optional["ColumnComparisonStatsCalculation"] = None


class ColumnComparisonStatsCalculation(BaseModel):
    """Data model for comparison statistics calculations (non-database)"""

    id: str = Field(default_factory=lambda: str(uuid4()))
    column_mapping_id: str

    # Row-level comparisons
    discarded_invalid_data: int = 0  # CRM has value, export doesn't
    added_new_data: int = 0  # CRM doesn't have value, export has value
    fixed_data: int = 0  # Both have values but different
    good_data: int = 0  # Both have values and same
    not_found: int = 0  # Both are null/empty

    # Correct values (unchanged)
    correct_values_before: int = 0  # In CRM
    correct_values_after: int = 0  # In Export
    correct_percentage_before: float = 0.0
    correct_percentage_after: float = 0.0

    # Format statistics
    crm_data_type: Optional[str] = None
    crm_format_count: int = 1
    export_data_type: Optional[str] = None
    export_format_count: int = 1


# Update forward references
ColumnMappingCalculation.model_rebuild()
EnrichmentReportCalculation.model_rebuild()
