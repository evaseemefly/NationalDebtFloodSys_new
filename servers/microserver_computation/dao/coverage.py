from typing import List, Optional, Any, Union

from sqlalchemy import distinct, select

from common.default import MS_UNIT
from common.enums import RasterFileType, TyphoonGroupEnum
from common.util import get_remote_url
from core.jobs import JobGenerateTyphoonPathFile, JobGenerateSurgeRasterPathFile
from dao.base import BaseDao
from models.models import TyphoonForecastGrouppath, TyphoonForecastRealdata, GeoCoverageFiles
from schema.coverage import CoverageFileInfoSchema
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathComplexSchema, TyphoonDistGroupSchema, TyphoonPointSchema


class BaseCoverageDao(BaseDao):
    def get_coveage_file_byparams(self, ty_code: str, issue_ts: int, raster_type: RasterFileType,
                                  group_type: TyphoonGroupEnum,
                                  **kwargs) -> Optional[CoverageFileInfoSchema]:
        """
            根据 预报 | 发布 时间戳 获取对应的 nc | tif 文件信息
        @param raster_type: 栅格图层种类 nc|tif
        @param area:        预报区域
        @param issue_ts:    发布时间戳
        @param kwargs:
        @return:
        """
        try:
            with self.session as session:
                stmt = select(GeoCoverageFiles).where(
                    GeoCoverageFiles.ty_code == ty_code,
                    GeoCoverageFiles.issue_ts == issue_ts,
                    GeoCoverageFiles.coverage_type == raster_type.value,
                    GeoCoverageFiles.group_type == group_type.value
                )
                res = session.execute(stmt).scalar_one_or_none()
                coverage_file_schema = CoverageFileInfoSchema.from_orm(res)
                return coverage_file_schema
        except Exception as ex:
            # Multiple rows were found when one or none was required
            print(ex)
            return None


class CoverageDao(BaseCoverageDao):
    def get_tif_file_url(self, ty_code: str, issue_ts: int, group_type: TyphoonGroupEnum,
                         coverage_type: RasterFileType = RasterFileType.GEOTIFF, **kwargs) -> str:
        """
            根据 code + task_id 获取对应的 tiff 文件
            若不存在则返回 ''
            eg:
                http://localhost:82/images/TYPHOON/data/user1/surge_path/2025/2106/1746777768768/
        @param issue_ts:
        @param coverage_type:栅格种类
        @param kwargs:forecast_ts:预报时间戳
        @return:
        """
        full_url: str = ''
        file_info: CoverageFileInfoSchema = self.get_coveage_file_byparams(ty_code, issue_ts, coverage_type,
                                                                           group_type)
        if file_info is not None:
            full_url = get_remote_url(file_info)

        return full_url

    pass
