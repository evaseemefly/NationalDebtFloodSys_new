from common.enums import TyphoonGroupEnum, NullEnum
from config.store_config import StoreConfig
from schema.coverage import CoverageFileInfoSchema

switch_dict = {'center': TyphoonGroupEnum.GROUP_CENTER,
               'fast': TyphoonGroupEnum.GROUP_FAST,
               'slow': TyphoonGroupEnum.GROUP_SLOW,
               'left': TyphoonGroupEnum.GROUP_LEFT,
               'right': TyphoonGroupEnum.GROUP_RIGHT}


def get_ty_group_enum(val: str) -> TyphoonGroupEnum:
    """
        根据 group stamp 获取对应的集合路径类型枚举
    @param val:
    @return:
    """
    return switch_dict.get(val, NullEnum)


def get_remote_url(file: CoverageFileInfoSchema) -> str:
    """
        根据传入的 file 基础信息获取对应的 remote_url
    @param file:
    @return:
    """
    host: str = StoreConfig.get_ip()
    area: str = 'images/TYPHOON/data'
    relative_url: str = f'{file.relative_path}/{file.file_name}'
    full_url: str = f'{host}/{area}/{relative_url}'
    # http: // localhost: 82 / images / nmefc_download /
    return full_url
