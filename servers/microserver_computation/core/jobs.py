from typing import List

import arrow
import pathlib
import xarray as xr
import pandas as pd
from sqlalchemy.orm import scoped_session, Session

from common.default import DEFAULT_RELATIVE_PATH, MS_UNIT
from common.enums import TyphoonForecastInstitutionEnum, RasterFileType, TyphoonGroupEnum, NullEnum
from common.util import get_ty_group_enum
from config.store_config import STORE_CONFIG
from core.transformers import SurgeTransformer
from models.mid_models import TyForecastRealDataMidModel, ForecastSurgeRasterFile
from models.models import TyphoonForecastDetailinfo, TyphoonForecastGrouppath, TyphoonForecastRealdata, \
    GeoCoverageFiles, TaskJobs, RelaGroupPathTask, RelaTaskFiles


class JobGenerateTyphoonPathFile:
    """
        生成 ty_pathfile 与 生成批处理文件
    """

    def __init__(self, user_id: int, ty_code: str, ty_name_en: str, ty_name_ch: str, timestamp: int):
        self.uid = user_id
        """用户id"""
        self.ty_code = ty_code
        """台风code"""
        self.ty_name_en = ty_name_en
        """台风名(英文)"""
        self.ty_name_ch = ty_name_ch
        """台风名(中文)"""
        self.timestamp = timestamp
        """提交作业时的时间戳"""
        self.root_path = STORE_CONFIG.get('STORE_ROOT_PATH')
        """读取的根目录"""

    @property
    def relative_path(self) -> str:
        """
            获取当前job存储文件的相对路径
            eg:
                user_xxx/ty_path/2024
        """
        relative_path_str: str = DEFAULT_RELATIVE_PATH
        user_stamp: str = 'user1'
        issue_arrow: arrow.Arrow = arrow.get(self.timestamp)
        issue_year: str = str(issue_arrow.datetime.year)
        relative_path_str = f'{user_stamp}/ty_path/{issue_year}/{str(self.ty_code)}'
        return relative_path_str

    def get_path_files(self) -> List[pathlib.Path]:
        """
            从当前路径中遍历台风路径文件，生成文件集合
        """
        target_path: pathlib.Path = pathlib.Path(self.root_path) / self.relative_path
        path_files: List[pathlib.Path] = []
        if target_path.exists():
            # 若指定路径存在则获取该路径下的所有文件
            path_files = [temp_file for temp_file in target_path.iterdir()]
            # for temp_file in target_path.iterdir():
            #     if temp_file.is_file():
            #         """
            #             文件样例:
            #                 tc_track_center.txt
            #         """
            #         temp_file_name: str = temp_file.name
            #         path_stamp: str = temp_file_name.split('.')[0].split('_')[2]
            #         """截取的台风路径标记 center|fast|slow|right|left"""
            #
            #         # 批量读取五个台风路径，分别写入db
            #         self.read_ty_path(str(temp_file))
        return path_files

    def read_ty_path(self, file_path: str) -> List[TyForecastRealDataMidModel]:
        """
            读取指定路径的台风路径文件并提取台风路径信息并返回
        """
        df: pd.DataFrame = None
        ty_realdata: List[TyForecastRealDataMidModel] = []
        if pathlib.Path(file_path).exists():
            # 使用空格分割;不使用header;指定读取的列名
            """
                eg: 
                'datetime', 'longitude', 'latitude', 'pressure', 'wind'
                2024090517,112.20000,19.20000,905.00000,26.00000
                2024090518,112.12000,19.30000,905.20000,26.10000
                2024090519,112.00000,19.40000,905.00000,26.00000
                2024090520,111.83000,19.50000,904.30000,25.80000
                2024090521,111.61000,19.61000,903.40000,25.70000

            """
            df = pd.read_csv(file_path, sep='\s+', header=None,
                             names=['datetime', 'longitude', 'latitude', 'pressure', 'wind'])
            # 逐行读取台风时间以及经纬度和气压以及最大风速
            for row in df.itertuples():
                # TODO[*] 25-04-30 此处的时间是世界时还是本地时间？
                # Pandas(Index=0, datetime=2024090517, longitude=112.2, latitude=19.2, pressure=905.0, wind=26.0)
                temp_dt_str: int = row.datetime
                temp_local_ar: arrow.Arrow = arrow.get(str(temp_dt_str), "YYYYMMDDHH", tzinfo="Asia/Shanghai")
                temp_utc_ar: arrow.Arrow = temp_local_ar.to('UTC')
                temp_ty_realdata = TyForecastRealDataMidModel(row.latitude, row.longitude, row.pressure,
                                                              temp_utc_ar.int_timestamp, '',
                                                              [])
                ty_realdata.append(temp_ty_realdata)

        return ty_realdata

    def to_do(self, session: scoped_session[Session]):
        """
            读取已经生成的台风集合路径，并 2 db
        """
        try:
            files: List[pathlib.Path] = self.get_path_files()
            # step1: 写入台风详情表
            timestamp_sec = int(self.timestamp / MS_UNIT)
            ty_detail: TyphoonForecastDetailinfo = TyphoonForecastDetailinfo(code=self.ty_code,
                                                                             name_ch=self.ty_name_ch,
                                                                             name_en=self.ty_name_en,
                                                                             forecast_source=TyphoonForecastInstitutionEnum.CMA.value,
                                                                             timestamp=timestamp_sec)
            session.add(ty_detail)
            # flush()：将更改同步到数据库，但不提交事务|commit(): 提交事务，使更改永久化
            session.flush()
            # session.commit()
            for temp_file in files:
                if temp_file.is_file():
                    """
                        文件样例:
                            tc_track_center.txt
                    """
                    temp_file_name: str = temp_file.name
                    path_stamp: str = temp_file_name.split('.')[0].split('_')[2]
                    """截取的台风路径标记 center|fast|slow|right|left"""

                    # 批量读取五个台风路径，分别写入db
                    temp_ty_realdata = self.read_ty_path(str(temp_file))
                    # step2: 写入台风 group path 表
                    temp_ty_grouppath: TyphoonForecastGrouppath = TyphoonForecastGrouppath(ty_id=ty_detail.id,
                                                                                           ty_code=ty_detail.code,
                                                                                           relative_path=self.relative_path,
                                                                                           file_name=temp_file_name,
                                                                                           ty_path_type=path_stamp,
                                                                                           timestamp=timestamp_sec)
                    session.add(temp_ty_grouppath)
                    session.flush()
                    # session.commit()
                    # step3: 将台风路径预报信息写入 ty realdata 表
                    list_ty_realdata: List[TyphoonForecastRealdata] = []
                    for index, val in enumerate(temp_ty_realdata):
                        ty_realdata_model = TyphoonForecastRealdata(ty_id=ty_detail.id, gp_id=temp_ty_grouppath.id,
                                                                    forecast_index=index, forecast_dt=val.forecast_dt,
                                                                    lat=val.lat, lon=val.lon, bp=val.bp,
                                                                    timestamp=val.ts)
                        list_ty_realdata.append(ty_realdata_model)
                    session.add_all(list_ty_realdata)
                    session.commit()
                    pass
            pass
        except Exception as ex:
            # TODO:[-] 25-05-07 ERROR: 'charmap' codec can't encode characters in position 0-1: character maps to <undefined>
            session.close()
            print(ex.args)


class JobGenerateSurgeRasterPathFile:
    """
        台风集合预报对应的增水栅格文件
    """

    def __init__(self, user_id: int, ty_code: str, ty_name_en: str, ty_name_ch: str, timestamp: int):
        self.uid = user_id
        """用户id"""
        self.ty_code = ty_code
        """台风code"""
        self.ty_name_en = ty_name_en
        """台风名(英文)"""
        self.ty_name_ch = ty_name_ch
        """台风名(中文)"""
        self.timestamp = timestamp
        """提交作业时的时间戳"""
        self.root_path = STORE_CONFIG.get('STORE_ROOT_PATH')
        """读取的根目录"""

    @property
    def relative_path(self) -> str:
        """
            获取当前job存储文件的相对路径
            eg:
                user1\surge_path\2025\2411\123456
        """
        relative_path_str: str = DEFAULT_RELATIVE_PATH
        user_stamp: str = 'user1'
        ty_arrow: arrow.Arrow = arrow.get(self.timestamp)
        # TODO:[*] 25-05-09
        # 测试方便此处手动修改为:'1746777768768'
        # ts_str: str = str(self.timestamp)
        ts_str: str = '1746777768768'
        ty_year: str = str(ty_arrow.datetime.year)
        relative_path_str = f'{user_stamp}/surge_path/{ty_year}/{str(self.ty_code)}/{ts_str}'
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
        # switch_dict = {'center': TyphoonGroupEnum.GROUP_CENTER,
        #                'fast': TyphoonGroupEnum.GROUP_FAST,
        #                'slow': TyphoonGroupEnum.GROUP_SLOW,
        #                'left': TyphoonGroupEnum.GROUP_LEFT,
        #                'right': TyphoonGroupEnum.GROUP_RIGHT}
        for temp_file in files:
            temp_group_type_str: str = temp_file.name.split('.')[0].split('_')[1]
            # temp_group_type: TyphoonGroupEnum = switch_dict.get(temp_group_type_str, NullEnum.NULL)
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
            暂时跳过——批量下载远端的预报产品
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

    def batch2db(self, session: scoped_session[Session], task_job: TaskJobs,
                 coverage_files: List[ForecastSurgeRasterFile]):
        """
            将 geotiff 批量写入db并于task表关联
        @param session:
        @param task_job:
        @param coverage_files:
        @return:
        """
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
        pass

    def create_coveragefiles(self, session: scoped_session[Session], task_job: TaskJobs,
                             coverage_files: List[ForecastSurgeRasterFile]):
        """
            基于 task 以及 ty_code 和 获取时间戳 获取并写入db 对应的 coverage file( geotiff | nc ) tb:geo_coverage_files
            并将与task的关系写入 tb:rela_task_files
        @param session: 数据库session
        @param task_job:当前的作业job
        @param coverage_files:栅格文件集合
        @return:
        """
        # step1: 获取所有的netcdf文件
        # step2: nc -> geotiff
        # step3: save -> db

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

    def to_do(self, session: scoped_session[Session], **kwargs):
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
        # step1: 批量下载(之后再实现)
        self.batch_download()
        # step2: 创建并写入db对应的nc文件信息
        self.create_coveragefiles(session, task_job, coverage_files)
        # step3: 批量将 nc->geotiff 并写入 db
        list_tiff_files = self.batch_nc2tiff(task_job, coverage_files)
        self.batch2db(session, task_job, list_tiff_files)
        pass
