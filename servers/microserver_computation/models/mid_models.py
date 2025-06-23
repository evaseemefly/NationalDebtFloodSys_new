import pathlib
from abc import abstractmethod
from typing import List
from datetime import datetime
import arrow
from common.enums import ElementTypeEnum, RasterFileType, TyphoonGroupEnum


class TyDetailMidModel:
    def __init__(self, ty_code: str, id: int, ty_name_en: str = None, ty_name_ch: str = None):
        self.ty_code = ty_code
        self.id = id
        self.ty_name_en = ty_name_en
        self.ty_name_ch = ty_name_ch

    def __str__(self) -> str:
        return f'TyDetailMidModel:id:{self.id}|ty_code:{self.ty_code}|name_en:{self.ty_name_en}|name_ch：{self.ty_name_ch}'


class TyForecastRealDataMidModelBackup:
    def __init__(self, lat: float, lon: float, bp: int, ts: int, ty_type: str, forecast_ty_path_list: []):
        """

        :param lat:
        :param lon:
        :param bp:
        :param ts:
        :param ty_type:
        """
        self.lat = lat
        self.lon = lon
        self.bp = bp
        self.ts = ts
        self.ty_type = ty_type
        self.forecast_ty_path_list = forecast_ty_path_list


class TyForecastRealDataMidModel:
    def __init__(self, lat: float, lon: float, bp: int, ts: int, ty_type: str,
                 forecast_ty_path_list: []):
        """

        :param lat:
        :param lon:
        :param bp:
        :param ts:
        :param ty_type:
        """
        self.lat = lat
        self.lon = lon
        self.bp = bp
        self.ts = ts
        self.ty_type = ty_type
        self.forecast_ty_path_list = []

    @property
    def forecast_dt(self) -> datetime:
        return arrow.get(self.ts).datetime


class TyPathMidModel:
    def __init__(self, ty_id: int, ty_code: str, ty_name_en: str, ty_name_ch: str,
                 ty_path_list: List[TyForecastRealDataMidModel] = []):
        """

        :param ty_id:
        :param ty_code:
        :param ty_name_en:
        :param ty_name_ch:
        :param ty_rate:
        :param ty_stamp:
        """
        self.ty_id = ty_id
        self.ty_code = ty_code
        self.ty_name_en = ty_name_en
        self.ty_name_ch = ty_name_ch
        self.ty_path_list = ty_path_list

        # self.ty_stamp = ty_stamp

    # @property
    # def ty_forecast_dt(self) -> datetime.datetime:
    #     return arrow.get(self.ty_stamp).datetime


class IForecastProductFile:
    """
        文件接口
    """

    def __init__(self,
                 file_name: str, issue_ts: int, relative_path: str, local_root_path: str,
                 remote_root_path: str = None, element_type: ElementTypeEnum = ElementTypeEnum.SURGE_MAX, ):
        """

        :param local_root_path: 本地存储的根目录(容器对应挂载volums根目录)——绝对路径
        :param element_type:  观测要素种类
        :param remote_root_path:    ftp下载的远端对应登录后的路径(相对路径)
        """
        self.element_type = element_type
        self.file_name = file_name
        self.issue_ts = issue_ts
        self.relative_path = relative_path
        """存储相对路径"""
        self.local_root_path = local_root_path
        """本地根目录"""
        self.remote_root_path = remote_root_path
        """ftp根目录"""

    @property
    def local_full_path(self) -> str:
        """
            本地存储全路径
        :return:
        """
        path = pathlib.Path(self.local_root_path) / self.relative_path / self.file_name
        return str(path)

    @property
    def remote_full_path(self) -> str:
        """
            ftp存储全路径
        :return:
        """
        path = pathlib.Path(self.remote_root_path) / self.relative_path / self.file_name
        return str(path)

    def exists(self):
        """
            判断当前 self.local_full_path 文件是否存在
        :return:
        """
        return pathlib.Path(self.local_full_path).exists()

    @property
    def issue_dt(self) -> arrow.Arrow:
        """
            -> get_issue_ts -> arrow.Arrow
        :return:
        """
        return arrow.get(self.issue_ts)


class ForecastSurgeRasterFile(IForecastProductFile):
    """
        预报增水场文件
        TODO:[!] 25-05-09 注意此处缺少 issue_ts
    """

    def __init__(self, raster_type: RasterFileType, issue_ts: int,
                 file_name: str, relative_path: str, local_root_path: str, ty_group_type: TyphoonGroupEnum):
        super().__init__(file_name, issue_ts, relative_path, local_root_path, element_type=ElementTypeEnum.SURGE_MAX)
        self.raster_file_type = raster_type
        self.ty_group_type = ty_group_type

    @property
    def local_full_path(self) -> str:
        """
            本地存储全路径
        :return:
        """
        path = pathlib.Path(self.local_root_path) / self.relative_path / self.file_name
        return str(path)

    def get_issue_ts(self) -> int:
        """
            根据存储目录获得发布时间
            path: /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024092312
               -> 2024092312
            24-10-16： /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024101400/nc_latlon/WNP
        :return:
        """
        # path = pathlib.Path(self.relative_path)
        relavtive_path_str: str = self.relative_path
        # last_dir_name: str = path.parent.name if path.is_file() else path.name
        # TODO:[-] 24-10-16 此处由于路径修改此处重新修改
        ts_str: str = relavtive_path_str.rsplit('/', 3)[1:2][0]
        """eg:2024092312"""
        ts: int = arrow.get(ts_str, 'YYYYMMDDHH').int_timestamp
        return ts

    def get_forecast_ts(self) -> int:
        """
            根据 file_name 生成发布时间
            file_name: field_2024-09-29_12_00_00.f0_WNP_standard_deflate.nc
                    -> 2024-09-29_12_00
        :return:
        """
        file_name: str = self.file_name
        # 获取时间戳
        file_date_stamp: str = file_name.split('.')[0]
        """文件中的包含时间的 截取 str
            eg: field_2024-09-29_12_00_00
        """
        date_stamp_str: str = file_date_stamp[6:]
        """eg: 2024-09-29_12_00_00"""
        # 转换为预报时间 utc 时间
        forecast_dt = arrow.get(date_stamp_str, 'YYYY-MM-DD_HH_mm_ss')
        forecast_ts: int = forecast_dt.int_timestamp
        return forecast_ts
