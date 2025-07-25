import pathlib

import geopandas as gpd
import rasterio
from rasterio import features
import numpy as np
import arrow
from typing import List

from commons.default import DEFAULT_RELATIVE_PATH
from commons.enums import TyphoonGroupEnum, RasterFileType
from config.base_config import StoreConfig
from core.transformers import SurgeTransformer
from db_factory import session_yield_scope
from mid_models.mid_models import ForecastSurgeRasterFile
from models.models import TaskJobs, GeoCoverageFiles
from util.utils import get_ty_group_enum


class SurgeRasterExecutor:
    """
        Executes raster ： 执行增水场栅格图层 执行器
        参考  源计算微服务中 core/jobs.py => JobGenerateSurgeRasterPathFile
        TODO:[-] 25-07-24 整合 JobGenerateSurgeRasterPathFile
    """

    def __init__(self, user_id: int, user_name: str, ty_code: str, ty_name_en: str, ty_name_ch: str, timestamp: int):
        self.uid = user_id
        """用户id"""
        self.user_name = user_name
        self.ty_code = ty_code
        """台风code"""
        self.ty_name_en = ty_name_en
        """台风名(英文)"""
        self.ty_name_ch = ty_name_ch
        """台风名(中文)"""
        self.timestamp = timestamp
        """提交作业时的时间戳"""
        self.root_path = StoreConfig.TY_SOURCE_PATH
        """读取的根目录"""

    @property
    def relative_path(self) -> str:
        """
            获取当前job存储文件的相对路径
            eg:
                /Volumes/DRCC_DATA/02WORKSPACE/nation_flood/model_execute  : TY_SOURCE_PATH
                surgeflood_wkdir/user_out                                  : STORE_REMOTE_RELATIVE_PATH
                /Volumes/DRCC_DATA/02WORKSPACE/nation_flood/model_execute/surgeflood_wkdir/user_out/admin
                user1\surge_path\2025\2411\123456
        """
        relative_path_str: str = DEFAULT_RELATIVE_PATH
        user_stamp: str = self.user_name
        ty_arrow: arrow.Arrow = arrow.get(self.timestamp)
        # TODO:[*] 25-05-09
        # 测试方便此处手动修改为:'1746777768768'
        # ts_str: str = str(self.timestamp)
        ts_str: str = '1746777768768'
        ty_year: str = str(ty_arrow.datetime.year)
        # TODO:[*] 25-07-25 此处不加入时间戳及年份
        # relative_path_str = f'{user_stamp}/surge_path/{ty_year}/{str(self.ty_code)}/{ts_str}'
        relative_path_str = f'{StoreConfig.STORE_REMOTE_RELATIVE_PATH}/{user_stamp}'
        return relative_path_str

    def get_path_files(self) -> List[pathlib.Path]:
        """
            从当前路径中遍历最大增水栅格文件，生成文件集合
        """
        target_path: pathlib.Path = pathlib.Path(self.root_path) / self.relative_path
        path_files: List[pathlib.Path] = []
        # eg: E:\05DATA\99test\TYPHOON\data\user1\surge_path\2025\2106\1746777768768
        if target_path.exists():
            # 若指定路径存在则获取该路径下的所有文件
            path_files = [temp_file for temp_file in target_path.iterdir()]
        return path_files

    def toDB(self, **kwargs):
        """
            将当前case对应的所有增水文件写入db
        """
        pass

    def get_coveragefiles(self) -> List[ForecastSurgeRasterFile]:
        """
            25-05-09
            获取当前路径下的所有栅格文件集合
        @param task_id:
        @param ty_code:
        @param issue_ts:
        @return:
        """
        coverage_files: List[ForecastSurgeRasterFile] = []
        files = self.get_path_files()

        for temp_file in files:
            temp_group_type_str: str = temp_file.name.split('.')[0].split('_')[1]
            temp_group_type: TyphoonGroupEnum = get_ty_group_enum(temp_group_type_str)
            """当前文件的集合路径枚举"""
            temp_relative_path: str = self.relative_path
            temp_coverage_file: ForecastSurgeRasterFile = ForecastSurgeRasterFile(RasterFileType.NETCDF, self.timestamp,
                                                                                  temp_file.name, temp_relative_path,
                                                                                  self.root_path, temp_group_type)
            coverage_files.append(temp_coverage_file)
        return coverage_files

    def nc2tiff(self, coverage_file: ForecastSurgeRasterFile):
        """
            将 nc 文件转换为 geotiff
        @param coverage_file:
        @return:
        """
        pass

    def batch_download(self):
        """
            @expired
            暂时跳过——批量下载远端的预报产品
            docker 与 本地映射 不需要单独下载
        @return:
        """
        pass

    def batch_nc2tiff(self, task_job: TaskJobs,
                      coverage_files: List[ForecastSurgeRasterFile]) -> List[ForecastSurgeRasterFile]:
        """
            将 nc 文件批量转换为 geotiff
        @param session:
        @param task_job:
        @param coverage_files:
        @return:
        """
        tiff_files: List[ForecastSurgeRasterFile] = []
        """转换后的geotiff文件集合"""
        for temp_file in coverage_files:
            temp_file_transformer = SurgeTransformer(temp_file, task_job.id)
            temp_file_transformer.read_data()
            temp_geo_tiff_file = temp_file_transformer.out_put()
            tiff_files.append(temp_geo_tiff_file)
        return tiff_files

    def batch2db(self, task_job: TaskJobs,
                 coverage_files: List[ForecastSurgeRasterFile]):
        """
            将 geotiff 批量写入db并于task表关联
        @param session:
        @param task_job:
        @param coverage_files:
        @return:
        """
        with session_yield_scope() as session:
            try:
                coverage_type: RasterFileType = RasterFileType.GEOTIFF
                list_geo_coverages: List[GeoCoverageFiles] = [
                    GeoCoverageFiles(task_id=task_job.id, ty_code=task_job.ty_code, relative_path=temp.relative_path,
                                     file_name=temp.file_name, issue_ts=temp.issue_ts, issue_dt=temp.issue_dt,
                                     coverage_type=coverage_type.value) for temp in
                    coverage_files]
                for temp_coverage in list_geo_coverages:
                    session.add(temp_coverage)
                    session.flush()
                    # TODO:[*] 25-05-13 暂时不写入关联表
                    # 写入关联表
                    # relation = RelaTaskFiles(task_id=task_job.id, file_id=temp_coverage.id, file_type=coverage_type.value)
                    # session.add(relation)
                session.commit()
            except Exception as ex:

                session.close()
                print(ex.args)
        pass

    def create_coveragefiles(self, task_job: TaskJobs,
                             coverage_files: List[ForecastSurgeRasterFile]):
        """
            基于 task 以及 ty_code 和 获取时间戳 获取并写入db 对应的 coverage file( geotiff | nc ) tb:geo_coverage_files
            并将与task的关系写入 tb:rela_task_files
        @param task_job:当前的作业job
        @param coverage_files:栅格文件集合
        @return:
        """
        # step1: 获取所有的netcdf文件
        # step2: nc -> geotiff
        # step3: save -> db

        with session_yield_scope() as session:
            coverage_type: RasterFileType = RasterFileType.NETCDF
            list_geo_coverages: List[GeoCoverageFiles] = [
                GeoCoverageFiles(task_id=task_job.id, ty_code=task_job.ty_code, relative_path=temp.relative_path,
                                 file_name=temp.file_name, issue_ts=temp.issue_ts, issue_dt=temp.issue_dt,
                                 coverage_type=coverage_type.value) for temp in
                coverage_files]
            for temp_coverage in list_geo_coverages:
                """
                    + 25-05-13
                    (MySQLdb._exceptions.OperationalError) (1054, "Unknown column 'forecast_time' in 'field list'")
                    [SQL: INSERT INTO geo_coverage_files (coverage_type, task_id, ty_code, is_del, relative_path, file_name, forecast_time, forecast_ts, issue_dt, issue_ts, gmt_create_time, gmt_modify_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)]
                    [parameters: (6101, -1, 'DEFAULT', 0, 'user1/surge_path/2025/2106/1746777768768', 'zmax_center.dat.nc', datetime.date(2025, 5, 13), 1747118753, <Arrow [2025-05-13T06:33:16.666000+00:00]>, 1747117996666, datetime.datetime(2025, 5, 13, 6, 48, 5, 151547), datetime.datetime(2025, 5, 13, 6, 48, 5, 151547))]
                    (Background on this error at: https://sqlalche.me/e/20/e3q8)
                """
                session.add(temp_coverage)
                session.flush()
                # # TODO:[*] 25-05-13 暂时不写入关联表
                # relation = RelaTaskFiles(task_id=task_job.id, file_id=temp_coverage.id, file_type=coverage_type.value)
                # session.add(relation)
            session.commit()
        pass

    def execute(self, **kwargs):
        """
            执行 coverage job
        @param session:
        @param kwargs:
        @return:
        """
        issue_ts: int = kwargs.get('issue_ts')
        files: List[pathlib.Path] = self.get_path_files()

        coverage_files: List[ForecastSurgeRasterFile] = self.get_coveragefiles()
        """当前路径下的所有栅格文件集合(只包含nc文件)"""
        # 获取对应的 task_jobs
        task_job = TaskJobs()
        # 本作业的主要处理流程
        # step1: 批量下载(之后再实现)—— docker 与 本地 volumns 映射，不需要下载文件
        # self.batch_download()
        # step2: 创建并写入db对应的nc文件信息
        self.create_coveragefiles(task_job, coverage_files)
        # step3: 批量将 nc->geotiff 并写入 db
        list_tiff_files = self.batch_nc2tiff(task_job, coverage_files)
        self.batch2db(task_job, list_tiff_files)
        pass
