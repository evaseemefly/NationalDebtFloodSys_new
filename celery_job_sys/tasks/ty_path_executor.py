import json
import pathlib
from datetime import datetime, timezone, timedelta
from typing import List
import pandas as pd

import arrow
from sqlalchemy.orm import scoped_session, Session

from commons.default import MS_UNIT
from commons.enums import TyphoonForecastInstitutionEnum
from config.base_config import StoreConfig
from db_factory import session_yield_scope
from mid_models.mid_models import TyForecastRealDataMidModel
from models.models import TyphoonForecastGrouppath, TyphoonForecastDetailinfo, TyphoonForecastRealdata
from schemas import TyphoonPathComplexDetailSchema, TyphoonPointSchema


class TyphoonPathExecutor:
    """
        台风路径执行者
    """

    def __init__(self, ty_path_schema: TyphoonPathComplexDetailSchema, out_put_path: str):
        self.ty_path_schema = ty_path_schema
        self.out_put_path = out_put_path

    def ty_detail_execute(self):
        """
            生成台风基础信息 json
        :param ty_path_schema:
        :param out_put:
        :return:
        """

        ty_detail = self.ty_path_schema.tyDetail
        ty_detail_json_content = {
            "tc_num": str(ty_detail.tyCode),
            "tc_name_en": ty_detail.tyNameEn,
            "tc_name_cn": ty_detail.tyNameCh
        }
        # --- 1. 生成 a.json 文件 ---
        print(f'正在生成 处理:{ty_detail.tyCode}台风路径->json')
        # 写入文件
        # ensure_ascii=False 确保中文字符正确显示
        # indent=4 使 JSON 文件格式优美，易于阅读
        file_name: str = 'tc_info.json'
        out_put_path: str = str(pathlib.Path(self.out_put_path) / file_name)
        with open(out_put_path, 'w', encoding='utf-8') as f:
            json.dump(ty_detail_json_content, f, ensure_ascii=False, indent=4)
        print(f"处理:{ty_detail.tyCode}台风detail->json 完毕{out_put_path}")

    def ty_path_list_execute(self):
        ty_detail = self.ty_path_schema.tyDetail
        ty_path: List[TyphoonPointSchema] = self.ty_path_schema.tyPathList
        # 定义北京时间时区 (UTC+8)
        cst_tz = timezone(timedelta(hours=8))

        file_name: str = 'tc_track_info.txt'
        out_put_path: str = str(pathlib.Path(self.out_put_path) / file_name)
        # --- 2. 生成 b.txt 文件 ---
        print(f"处理:{ty_detail.tyCode}台风路径->json ing")
        # 使用 with open 确保文件操作安全
        with open(out_put_path, 'w', encoding='utf-8') as f:
            # 写入文件头
            f.write("dateCST lonTC latTC presTC\n")

            # 遍历路径列表
            for path_point in ty_path:
                utc_dt = path_point.forecastDt
                # 直接将其转换为北京时间
                cst_dt = utc_dt.astimezone(cst_tz)
                # 格式化时间为 YYYYMMDDHH 字符串
                date_cst_str = cst_dt.strftime('%Y%m%d%H')
                # 提取经纬度和气压
                lon = path_point.lon
                lat = path_point.lat
                bp = path_point.bp
                # 格式化为目标字符串并写入文件
                # 经纬度保留一位小数，气压取整
                line = f"{date_cst_str} {lon:.1f} {lat:.1f} {int(bp)}\n"
                f.write(line)

        print(f"处理:{ty_detail.tyCode}台风路径->json 完毕{out_put_path}")


class TyphoonGroupPathExecutor:
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
        self.root_path = StoreConfig.STORE_ROOT_PATH
        """读取的根目录"""

    @property
    def relative_path(self) -> str:
        """
            获取当前job存储文件的相对路径
            eg:
                user_xxx/ty_path/2024
        """
        relative_path_str: str = StoreConfig.STORE_REMOTE_RELATIVE_PATH
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

    def execute(self) -> None:
        """
            读取已经生成的台风集合路径，并 2 db
        """
        with session_yield_scope() as session:
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
                                                                        forecast_index=index,
                                                                        forecast_dt=val.forecast_dt,
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
