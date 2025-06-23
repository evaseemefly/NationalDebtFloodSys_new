from datetime import datetime

from pydantic import BaseModel


class CoverageFileInfoSchema(BaseModel):
    """
        栅格 file 基础信息
    """
    relative_path: str
    file_name: str
    group_type: int
    task_id: int
    ty_code: str

    class Config:
        orm_mode = True
