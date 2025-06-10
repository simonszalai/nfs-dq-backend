from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from sqlalchemy import TIMESTAMP
from sqlmodel import JSON, Column, Field, Relationship, SQLModel


class EnrichmentReport(SQLModel, table=True):
    """Database model for enrichment reports"""

    __tablename__ = "EnrichmentReport"  # type: ignore

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    token: str = Field(unique=True)
    filename: str
    created_at: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True)),
        default_factory=lambda: datetime.now(timezone.utc),
    )

    # Basic run information
    total_rows: int
    total_crm_columns: int
    total_export_columns: int

    # Global statistics
    new_columns_count: int
    many_to_one_count: int
    columns_reduced_by_merging: int
    records_modified_count: int
    export_columns_created: int

    # Relationships
    column_mappings: List["ColumnMapping"] = Relationship(
        back_populates="enrichment_report"
    )


class ColumnMapping(SQLModel, table=True):
    """Database model for column mappings"""

    __tablename__ = "ColumnMapping"  # type: ignore

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    enrichment_report_id: str = Field(foreign_key="EnrichmentReport.id")

    # Column names
    crm_column: str
    export_column: Optional[str] = None

    # Mapping details
    is_many_to_one: bool = False
    additional_crm_columns: Optional[List[str]] = Field(
        default=None, sa_column=Column(JSON)
    )
    confidence: float
    reasoning: str

    # Relationships
    enrichment_report: EnrichmentReport = Relationship(back_populates="column_mappings")
    comparison_stats: Optional["ColumnComparisonStats"] = Relationship(
        back_populates="column_mapping"
    )


class ColumnComparisonStats(SQLModel, table=True):
    """Database model for column comparison statistics"""

    __tablename__ = "ColumnComparisonStats"  # type: ignore

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    column_mapping_id: str = Field(foreign_key="ColumnMapping.id")

    # Row-level comparisons
    discarded_invalid_data: int = 0
    added_new_data: int = 0
    fixed_data: int = 0
    good_data: int = 0

    # Correct values
    correct_values_before: int = 0
    correct_values_after: int = 0
    correct_percentage_before: float = 0.0
    correct_percentage_after: float = 0.0

    # Format statistics
    crm_data_type: Optional[str] = None
    crm_format_count: int = 1
    export_data_type: Optional[str] = None
    export_format_count: int = 1

    # Relationship
    column_mapping: ColumnMapping = Relationship(back_populates="comparison_stats")
