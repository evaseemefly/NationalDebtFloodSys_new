import arrow
import os
import pathlib
import pandas as pd

from commons.enums import TYGroupTypeEnum
from db_factory import session_yield_scope
from models import StationForecastRealdataModel, StationAstronomicTide
from util.file_util import get_grouppath_type, FileExplorer


class StationForecastDataProcessor:
    def __init__(self, read_path: str, dict_map: dict):
        """
        初始化数据处理器
        :param database_url: 数据库连接字符串，如 'mysql+pymysql://user:password@host:port/database'
        """
        self.read_path = read_path
        self.dict_map = dict_map
        """code:name的map映射字典"""

    def process_files_from_path(self, ):
        """
        从指定路径读取五个.dat文件并处理入库
        :param file_path: 文件路径
        :param user_id: 用户ID
        """
        try:
            # 获取路径下所有.dat文件
            file_explorer = FileExplorer(self.read_path)
            files = file_explorer.get_all_files()
            print(f"警告：路径下找到{len(files)}个等待录入文件")
            processed_files = []
            # with session_yield_scope() as session:
            for temp_file in files:  # 只处理前5个文件

                temp_file_name = temp_file.name
                full_path = str(pathlib.Path(self.read_path) / temp_file_name)
                print(f"正在处理文件: {temp_file_name}")

                station_name: str = temp_file_name.split('.')[0]
                station_code: str = self.dict_map.get(station_name, 'DEFAULT')
                if station_code == 'DEFAULT':
                    continue
                # 处理单个文件
                # success = self._process_single_file(full_path, file_name, center_identifier)
                success = self._process_single_file_with_pandas(full_path, temp_file_name, station_code)

                if success:
                    processed_files.append(temp_file_name)

            print(f"成功处理 {len(processed_files)} 个文件: {processed_files}")

        except Exception as e:
            # self.session.rollback()
            print(f"处理文件时发生错误: {str(e)}")
            raise

    def _process_single_file_with_pandas(self, file_path, file_name, station_code: str):
        """
            直接使用pd.read_csv的方式读取站点增水文件
        """
        is_ok: bool = False
        try:
            # station_code: str = 'DEFAULT'

            df = pd.read_csv(
                file_path,
                sep=r',',  # 使用正则表达式匹配一个或多个空白字符
                encoding='utf-8',
                engine='python'  # 使用python引擎支持正则表达式
            )

            print(f"文件 {file_name} 读取成功，数据形状: {df.shape}")
            print(f"列名: {list(df.columns)}")

            # 检查数据是否为空
            if df.empty:
                print(f"文件 {file_name} 为空，跳过处理")
                return False

            # TODO:*] 25-06-30 No module named 'MySQLdb'
            with session_yield_scope() as session:
                try:
                    list_models = []
                    row = df.shape[0]
                    """行数"""
                    col = df.shape[1]
                    """列数"""
                    for i in range(row):
                        temp_row = df.iloc[i]
                        temp_dt_utc_str = str(temp_row['date_UTC'])
                        temp_arrow: arrow.Arrow = arrow.get(temp_dt_utc_str, 'YYYYMMDDHH')
                        temp_dt = temp_arrow.datetime
                        temp_ts: int = temp_arrow.int_timestamp
                        temp_tide = temp_row['tide_cm']
                        # 获取该站名对应的 code
                        # self.dict_map.get(station_code).add(temp_tide)
                        temp_model = StationAstronomicTide(station_code=station_code, ts=temp_ts, tide=temp_tide,
                                                           gmt_realtime=temp_dt)
                        list_models.append(temp_model)
                    session.add_all(list_models)
                    """
                        (pymysql.err.OperationalError) (1049, "Unknown database 'surge_global_sys'")
                        [SQL: INSERT INTO surge_global_sys.station_astronomic_tide (is_del, gmt_create_time, gmt_modify_time, station_code, gmt_realtime, tide, ts) VALUES (%(is_del)s, %(gmt_create_time)s, %(gmt_modify_time)s, %(station_code)s, %(gmt_realtime)s, %(tide)s, %(ts)s)]
                        [parameters: {'is_del': 0, 'gmt_create_time': None, 'gmt_modify_time': None, 'station_code': 'LHDAO', 'gmt_realtime': None, 'tide': 376.0, 'ts': 1704038400}]
                    """
                    session.commit()
                    print(f'处理文件{file_name}结束，共插入{len(list_models)}rows 数据~')
                    is_ok = True
                except Exception as e:
                    print(f"处理文件 {file_name} 时发生未知错误: {str(e)}")
                    session.close()
            return is_ok

        except pd.errors.EmptyDataError:
            print(f"文件 {file_name} 为空文件，跳过处理")
            return False
        except pd.errors.ParserError as e:
            print(f"解析文件 {file_name} 时发生错误: {str(e)}")
            return False
        except Exception as e:
            print(f"处理文件 {file_name} 时发生未知错误: {str(e)}")
            return False


class StationBaseInfoProcessor:
    def __init__(self, read_path: str):
        self.read_path = read_path

    def to_dict(self, ) -> dict:
        df = pd.read_csv(
            self.read_path,
            sep=r',',  # 使用正则表达式匹配一个或多个空白字符
            encoding='utf-8',
            engine='python'  # 使用python引擎支持正则表达式
        )
        dict_map = {}
        for index in range(df.shape[0]):
            # print(index, val)
            temp_name = df.loc[index, 'staNameCn']
            temp_code = df.loc[index, 'staNameEn']
            dict_map[temp_name] = temp_code
        return dict_map


# 使用示例
def main():
    # 文件路径
    READPATH: str = r'/Users/evaseemefly/03data/03项目数据/广东国债项目/tide_NMEFC/tide_NMEFC/2024'

    read_station_base_info_path: str = r'/Users/evaseemefly/03data/03项目数据/广东国债项目/station_info_latlon.csv'

    station_base = StationBaseInfoProcessor(read_station_base_info_path)
    dict_map: dict = station_base.to_dict()
    """code:name的map映射字典"""
    # 创建处理器
    processor = StationForecastDataProcessor(READPATH, dict_map)

    try:
        # 处理文件
        processor.process_files_from_path()
        print("数据处理完成！")

    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")

    finally:
        # processor.close()
        pass


if __name__ == "__main__":
    main()
