import pathlib
from typing import List, Optional

import arrow
import pandas as pd

# 假设这些是您项目中的模块，保持不变
from commons.enums import TYGroupTypeEnum
from db_factory import session_yield_scope
from models.models import StationForecastRealdataModel
from schemas import StationSurgeFileSchema
from util.file_util import FileExplorer


class StationSurgeExecutor:
    """
    一个用于处理和入库站点增水预报数据的执行器类。
    该类封装了从指定目录读取数据文件、解析数据、并将其批量写入数据库的全部逻辑。
    """

    def __init__(self, read_path: str, ty_code: str, issue_ts: int, forecast_start_ts: int):
        """
           初始化执行器。
        :param read_path: 存放 .dat 文件的目录路径。
        :param ty_code: 当前处理的台风编号。
        :param issue_ts: 预报发布时间的 Unix 时间戳。
        :param forecast_start_ts:  预报起始时间的 Unix 时间戳。
        """
        self.read_path = pathlib.Path(read_path)
        self.ty_code = ty_code
        self.issue_ts = issue_ts
        self.forecast_start_ts = forecast_start_ts

        if not self.read_path.is_dir():
            raise FileNotFoundError(f"指定的读取路径不存在或不是一个目录: {self.read_path}")

    def execute(self) -> None:
        """
        执行主流程：获取文件 -> 循环处理 -> 写入数据库。
        """
        print(f"[*] 开始执行站点增水数据处理任务...")
        print(f"    - 台风编号: {self.ty_code}")
        print(f"    - 数据路径: {self.read_path}")

        files = self._get_source_files()
        if not files:
            print(f"[!] 在路径 {self.read_path} 中未找到任何文件。任务终止。")
            return

        print(f"[*] 发现 {len(files)} 个文件，开始逐一处理。")
        for index, file_path in enumerate(files):
            print(f"\n--- [{index + 1}/{len(files)}] 正在处理文件: {file_path.name} ---")

            # 使用实例属性构建 Schema
            file_schema = StationSurgeFileSchema(
                ty_code=self.ty_code,
                issue_ts=self.issue_ts,
                forecast_ts=self.forecast_start_ts,
                group_id=index,
                file_name=file_path.name
            )

            # 1. 读取文件到 DataFrame
            df = self._read_dat_to_dataframe(file_path)
            if df is None or df.empty:
                print(f"[!] 读取文件 {file_path.name} 失败或文件为空，已跳过。")
                continue

            # 2. 将 DataFrame 数据写入数据库
            self._save_dataframe_to_db(df, file_schema)

        print("\n[*] 所有文件处理完毕。")

    def _get_source_files(self) -> List[pathlib.Path]:
        """从指定路径获取所有文件列表。"""
        try:
            explorer = FileExplorer(str(self.read_path))
            return explorer.get_all_files()
        except Exception as e:
            print(f"[!] 获取文件列表时发生错误: {e}")
            return []

    @staticmethod
    def _read_dat_to_dataframe(file_path: pathlib.Path) -> Optional[pd.DataFrame]:
        """
        将单个 .dat 文件读取为 pandas DataFrame。
        这是一个静态方法，因为它不依赖于任何实例状态。
        """
        try:
            # 使用 pandas 直接读取，指定分隔符为一个或多个空格，并将第一行作为表头
            df = pd.read_csv(file_path, sep=r'\s+', header=0)
            return df
        except Exception as e:
            print(f"[!] 读取文件 {file_path.name} 时出错: {e}")
            return None

    @staticmethod
    def _convert_dataframe_to_dict(df: pd.DataFrame) -> dict:
        """
        将 DataFrame 转换为以站点名为键，增水序列为值的字典。
        这是一个静态方法，因为它不依赖于任何实例状态。
        """
        station_data_dict = {}
        for col_name in df.columns:
            station_data_dict[col_name] = df[col_name].tolist()
        return station_data_dict

    def _save_dataframe_to_db(self, df: pd.DataFrame, file_schema: StationSurgeFileSchema) -> None:
        """
        将 DataFrame 中的数据批量存入数据库。
        """
        station_forecast_dict = self._convert_dataframe_to_dict(df)
        forecast_dt_start = arrow.get(file_schema.forecast_ts)
        TIME_STEP_HOURS = 1  # 每条记录的时间步长为1小时

        with session_yield_scope() as session:
            for station_code, surge_values in station_forecast_dict.items():
                records_to_add: List[StationForecastRealdataModel] = []
                try:
                    for index, surge_value in enumerate(surge_values):
                        # 计算每个预报点的时间
                        current_forecast_dt = forecast_dt_start.shift(hours=index * TIME_STEP_HOURS)
                        current_forecast_ts = current_forecast_dt.int_timestamp

                        # 创建数据库模型实例
                        record = StationForecastRealdataModel(
                            ty_code=file_schema.ty_code,
                            gp_id=file_schema.group_id,  # 使用 schema 中的 group_id
                            station_code=station_code,
                            timestamp=file_schema.issue_ts,  # 发布时间
                            forecast_dt=current_forecast_dt.datetime,  # 预报时间
                            forecast_ts=current_forecast_ts,
                            forecast_index=index,
                            surge=surge_value
                        )
                        records_to_add.append(record)

                    # 批量添加一个站点的所有预报数据
                    session.add_all(records_to_add)
                    session.commit()
                    print(f"    - 成功写入站点 [{station_code}] 的 {len(records_to_add)} 条数据。")
                except Exception as ex:
                    print(f"    - [!] 写入站点 [{station_code}] 数据时失败: {ex}")
                    session.rollback()  # 如果失败，回滚事务，防止部分写入


# --- 主程序入口 ---
def main():
    """
    主函数，用于演示如何使用 StationSurgeExecutor 类。
    """
    # 1. 设置运行参数
    READ_PATH = r'E:\01data\99test\flood'
    # 注意：这些参数在实际应用中应动态获取
    TY_CODE = '2106'
    ISSUE_TS = 1747125125  # 预报发布时间戳
    FORECAST_START_TS = 1725526800  # 预报起始时间戳

    # 2. 实例化执行器
    try:
        executor = StationSurgeExecutor(
            read_path=READ_PATH,
            ty_code=TY_CODE,
            issue_ts=ISSUE_TS,
            forecast_start_ts=FORECAST_START_TS
        )
        # 3. 执行任务
        executor.execute()
    except FileNotFoundError as e:
        print(f"错误：{e}")
    except Exception as e:
        print(f"程序运行期间发生未知错误: {e}")


if __name__ == '__main__':
    main()
