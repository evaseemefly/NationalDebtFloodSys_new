from pydantic import BaseModel


class TyGroupTaskSchema(BaseModel):
    ty_code: str
    status: int
    issue_ts: int

    class Config:
        orm_mode = True  # 旧版本
        # from_attributes = True  # 新版本
