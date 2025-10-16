from pydantic import BaseModel, Field

class ExportConfig(BaseModel):
    dsn: str
    sql: str
    output: str
    chunk_size: int = 10_000
    delimiter: str = ","
    quotechar: str = '"'
    header: bool = True

class BackupConfig(BaseModel):
    dsn: str
    output: str

class AnalyzeConfig(BaseModel):
    dsn: str
