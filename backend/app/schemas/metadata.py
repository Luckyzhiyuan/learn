"""元数据模块 schema（Spec §3.2）。"""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.schemas import ORMModel


class DatabaseOut(ORMModel):
    id: int
    db_name: str
    layer: str = ""
    owner: str = ""
    location: str = ""
    remark: str = ""


class TableItem(ORMModel):
    id: int
    table_name: str
    comment: str = ""
    db_name: str = ""
    table_type: str = "MANAGED"
    storage_format: str = "ORC"
    compress: str = "SNAPPY"
    is_partitioned: int = 0
    row_count: int = 0
    table_size_bytes: int = 0
    file_count: int = 0
    little_file_count: int = 0
    owner: str = ""


class ColumnOut(ORMModel):
    id: int
    table_id: int
    column_name: str
    column_type: str = ""
    comment: str = ""
    is_partition: int = 0
    is_primary: int = 0
    ordinal: int = 0


class PartitionOut(ORMModel):
    id: int
    table_id: int
    partition_key: str
    partition_value: str
    size_bytes: int = 0
    row_count: int = 0
    file_count: int = 0


class TableDetailOut(ORMModel):
    id: int
    db_name: str = ""
    table_name: str
    comment: str = ""
    table_type: str = "MANAGED"
    storage_format: str = "ORC"
    compress: str = "SNAPPY"
    is_partitioned: int = 0
    row_count: int = 0
    table_size_bytes: int = 0
    file_count: int = 0
    little_file_count: int = 0
    owner: str = ""
    location: str = ""
    source_mode: str = "file"
    db_id: int = 0
    columns: list[ColumnOut] = Field(default_factory=list)
    partitions: list[PartitionOut] = Field(default_factory=list)
    created_at: datetime | None = None


class LineageNode(BaseModel):
    id: int
    label: str
    type: str = "table"


class LineageEdge(BaseModel):
    src_id: int
    dst_id: int
    task_id: int | None = None
    project_id: int | None = None
    job_type: str = ""
    owner: str = ""
    schedule_status: str = ""
    schedule_time: datetime | None = None


class LineageOut(BaseModel):
    nodes: list[LineageNode] = Field(default_factory=list)
    edges: list[LineageEdge] = Field(default_factory=list)


class MetadataSyncReq(BaseModel):
    mode: str = "all"
    scope: list[str] = Field(default_factory=list)
    source: str = "hive"


class MetadataSyncOut(BaseModel):
    job_id: str
    status: str = "SUBMITTED"