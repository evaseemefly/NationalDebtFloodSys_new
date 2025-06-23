from pydantic import BaseModel


class StionInfoSchema(BaseModel):
    station_name: str
    station_code: str
    lat: float
    lon: float
    desc: str

    class Config:
        orm_mode = True  # 旧版本
