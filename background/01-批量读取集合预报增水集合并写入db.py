import arrow
from sqlalchemy import create_engine, Column, BigInteger, String, Integer, DateTime, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import pathlib
import pandas as pd

from commons.enums import TYGroupTypeEnum
from db_factory import session_yield_scope
from models import StationForecastRealdataModel
from util.file_util import get_grouppath_type


class StationForecastDataProcessor:
    def __init__(self, database_url, ty_code: str, issue_ts: int, forecast_start_ts: int):
        """
        初始化数据处理器
        :param database_url: 数据库连接字符串，如 'mysql+pymysql://user:password@host:port/database'
        """
        # self.engine = session_yield_scope()
        # Base.metadata.create_all(self.engine)
        # Session = session_yield_scope()
        self.ty_code = ty_code
        self.issue_ts = issue_ts
        self.forecast_start_ts = forecast_start_ts
        # self.session = Session

    def process_files_from_path(self, file_path, user_id='user1'):
        """
        从指定路径读取五个.dat文件并处理入库
        :param file_path: 文件路径
        :param user_id: 用户ID
        """
        try:
            # 获取路径下所有.dat文件
            dat_files = [f for f in os.listdir(file_path) if f.endswith('.dat')]

            if len(dat_files) != 5:
                print(f"警告：路径下找到{len(dat_files)}个.dat文件，期望5个文件")
            processed_files = []
            # with session_yield_scope() as session:
            for file_name in dat_files:  # 只处理前5个文件

                full_path = str(pathlib.Path(file_path) / file_name)
                print(f"正在处理文件: {file_name}")

                # 解析文件名获取中间路径标识
                center_identifier = self._extract_center_from_filename(file_name)

                # 处理单个文件
                # success = self._process_single_file(full_path, file_name, center_identifier)
                success = self._process_single_file_with_pandas(full_path, file_name, center_identifier)

                if success:
                    processed_files.append(file_name)

                    # 创建用户与表的关联关系
                    # self._create_user_table_relation(user_id, f'station_forecast_realdata_{user_id}', file_name)

                # session.add_all(processed_files)
                # session.commit()
            print(f"成功处理 {len(processed_files)} 个文件: {processed_files}")

        except Exception as e:
            # self.session.rollback()
            print(f"处理文件时发生错误: {str(e)}")
            raise

    def _extract_center_from_filename(self, filename):
        """
        从文件名中提取中间路径标识
        例如: station_output_center.dat -> center
        """
        parts = filename.replace('.dat', '').split('_')
        if len(parts) >= 3:
            return parts[-1]  # 返回最后一部分作为center标识
        return 'unknown'

    def _process_single_file(self, file_path, file_name, center_identifier):
        """
        处理单个.dat文件
        """
        try:
            # 读取文件
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if len(lines) < 2:
                print(f"文件 {file_name} 格式不正确，跳过处理")
                return False

            # 解析表头（站点代码）
            header_line = lines[0].strip()
            station_codes = header_line.split()[1:]  # 跳过第一个'fhour'列

            # 清理可能存在的重复站点代码
            unique_stations = {}
            for i, code in enumerate(station_codes):
                if code not in unique_stations:
                    unique_stations[code] = []
                unique_stations[code].append(i)

            # 处理数据行
            for line in lines[1:]:
                line = line.strip()
                if not line:
                    continue

                values = line.split()
                if len(values) < 2:
                    continue

                fhour = values[0]
                forecast_values = values[1:]

                # 为每个站点插入数据
                for station_code, indices in unique_stations.items():
                    # 如果有重复站点，取第一个值
                    if indices[0] < len(forecast_values):
                        forecast_value = forecast_values[indices[0]]

                        # 创建数据记录
                        record = StationForecastRealdataModel(
                            ty_code=self.ty_code,
                            station_code=station_code,
                            station_name=station_code,  # 如果没有中文名，使用代码
                            surge=forecast_value,
                            data_source=f"{file_name}_{center_identifier}"
                        )

                        self.session.add(record)

            return True

        except Exception as e:
            print(f"处理文件 {file_name} 时发生错误: {str(e)}")
            return False

    def _process_single_file_with_pandas(self, file_path, file_name, center_identifier):
        """
            直接使用pd.read_csv的方式读取站点增水文件
        """
        try:
            #
            df = pd.read_csv(
                file_path,
                sep=r'\s+',  # 使用正则表达式匹配一个或多个空白字符
                encoding='utf-8',
                engine='python'  # 使用python引擎支持正则表达式
            )

            print(f"文件 {file_name} 读取成功，数据形状: {df.shape}")
            print(f"列名: {list(df.columns)}")

            grouppath_type: TYGroupTypeEnum = get_grouppath_type(center_identifier)
            """根据center_identifier-> 集合路径枚举 """
            # 检查数据是否为空
            if df.empty:
                print(f"文件 {file_name} 为空，跳过处理")
                return False

            # TODO:[-] 25-06-19 注意小时列title为 fhour
            # 获取fhour列（第一列）和站点列
            fhour_column = df.columns[0]  # 第一列是fhour
            station_columns = df.columns[1:]  # 其余列是站点代码
            # TODO:[*] 25-06-19 先测试使用前5个
            station_columns = station_columns[:5]

            print(f"预报时间列: {fhour_column}")
            print(f"站点数量: {len(station_columns)}")
            print(f"数据行数: {len(df)}")

            with session_yield_scope() as session:
                # 遍历每个站点列
                for station_code in station_columns:
                    # 批量插入数据
                    records_to_insert = []
                    # forecast_value = row[station_code]
                    series_temp = df[station_code]
                    forecast_start_ts: int = self.forecast_start_ts
                    forecast_start_arrow: arrow.Arrow = arrow.get(forecast_start_ts)
                    issue_ts: int = self.issue_ts

                    # 遍历列向量并批量写入
                    for index, temp_val in enumerate(series_temp):
                        temp_forecast_arrow = forecast_start_arrow.shift(hours=index)
                        temp_forecast_dt = temp_forecast_arrow.datetime
                        temp_forecast_ts: int = temp_forecast_arrow.int_timestamp
                        # 处理NaN值
                        # 25-06-19 替换为三元运算符
                        # if pd.isna(temp_val):
                        #     forecast_value: float = 0.0
                        # else:
                        #     forecast_value: float = temp_val

                        # 此处修改为使用三元运算符
                        forecast_value = 0.0 if pd.isna(temp_val) else temp_val

                        # 创建数据记录
                        record = StationForecastRealdataModel(
                            ty_code=self.ty_code,
                            station_code=station_code,
                            surge=forecast_value,
                            issue_time=issue_ts,
                            forecast_dt=temp_forecast_dt,
                            forecast_ts=temp_forecast_ts,
                            forecast_index=index,
                            grouppath_type=grouppath_type.value
                        )

                        records_to_insert.append(record)
                    session.add_all(records_to_insert)
                    session.commit()
                    print(f'处理当前站点:{station_code}完成!')
                    # 批量插入到数据库
                    if len(records_to_insert) > 0:
                        print(f"文件 {file_name} 准备插入 {len(records_to_insert)} 条记录")

            return True

        except pd.errors.EmptyDataError:
            print(f"文件 {file_name} 为空文件，跳过处理")
            return False
        except pd.errors.ParserError as e:
            print(f"解析文件 {file_name} 时发生错误: {str(e)}")
            return False
        except Exception as e:
            print(f"处理文件 {file_name} 时发生未知错误: {str(e)}")
            return False

    # def _create_user_table_relation(self, user_id, table_name, source_file):
    #     """
    #     创建用户与表的关联关系
    #     """
    #     try:
    #         # 检查是否已存在关联
    #         existing = self.session.query(RelaUserStationForecast).filter_by(
    #             user_id=user_id,
    #             station_forecast_table_name=table_name
    #         ).first()
    #
    #         if not existing:
    #             relation = RelaUserStationForecast(
    #                 user_id=user_id,
    #                 station_forecast_table_name=table_name,
    #                 relation_type=1,  # 订阅类型
    #                 status=1,  # 启用状态
    #                 priority=0,
    #                 remark=f"自动创建，数据源文件: {source_file}"
    #             )
    #             self.session.add(relation)
    #             print(f"创建用户 {user_id} 与表 {table_name} 的关联关系")
    #
    #     except Exception as e:
    #         print(f"创建关联关系时发生错误: {str(e)}")

    # def close(self):
    #     """关闭数据库连接"""
    #     self.session.close()


# 使用示例
def main():
    # 文件路径
    READPATH: str = r'E:\01data\99test\station_surge'
    ty_code: str = '2106'
    issue_ts: int = 1725584400
    forecast_ts: int = 1725584400

    # 创建处理器
    processor = StationForecastDataProcessor('', ty_code, issue_ts, forecast_ts)

    try:
        # 处理文件
        processor.process_files_from_path(READPATH, user_id='user1')
        print("数据处理完成！")

    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")

    finally:
        processor.close()


if __name__ == "__main__":
    main()
