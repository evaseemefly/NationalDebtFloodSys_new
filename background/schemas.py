from pydantic import BaseModel

from commons.enums import TYGroupTypeEnum

group_path_switch = {
    'center': TYGroupTypeEnum.CENTER,
    'fast': TYGroupTypeEnum.FAST,
    'left': TYGroupTypeEnum.LEFT,
    'right': TYGroupTypeEnum.RIGHT,
    'slow': TYGroupTypeEnum.SLOW
}


def get_group_type(val: str) -> TYGroupTypeEnum:
    return group_path_switch.get(val)


class StationSurgeFileSchema(BaseModel):
    ty_code: str
    issue_ts: int
    forecast_ts: int
    group_id: int
    file_name: str

    # group_path_stamp: str

    @property
    def group_path_stamp(self) -> str:
        # {AttributeError}AttributeError("type object 'StationSurgeFileSchema' has no attribute 'file_name'")
        path_stamp: str = self.file_name.split('.')[0].split('_')[-1]
        return path_stamp

    @property
    def group_path_type(self) -> TYGroupTypeEnum:
        path_type: TYGroupTypeEnum = get_group_type(self.group_path_stamp())
        return path_type


class TyGroupTaskSchema(BaseModel):
    ty_code: str
    status: int
    issue_ts: int

    class Config:
        orm_mode = True  # 旧版本
        # from_attributes = True  # 新版本
