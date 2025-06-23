from typing import List

import arrow
import pandas as pd
import numpy as np
import pathlib

from commons.enums import TYGroupTypeEnum
from db_factory import session_yield_scope
from models import Station, StationForecastRealdataModel
from schemas import StationSurgeFileSchema
from util.file_util import FileExplorer

READPATH: str = r'E:\01data\99test\flood'


def read_dat_2_df(file: pathlib.Path) -> pd.DataFrame:
    try:
        # 使用pandas直接读取，指定分隔符为空格
        df = pd.read_csv((file), sep='\s+', header=0)
        return df
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return None


def read_df_2_dict(df: pd.DataFrame) -> dict:
    dict_df: dict = {}
    for col in df.columns:
        station_name: str = col
        temp_series: pd.Series = df[col]
        dict_df[station_name] = temp_series.tolist()
    return dict_df


def batch_df_2_db(df: pd.DataFrame, ty_code: str, forecast_ts: int, issue_ts: int,
                  group_path_type: TYGroupTypeEnum) -> None:
    """
        将 dataframe -> dict -> 批量 commit
    :param df:
    :param ty_code:
    :param forecast_ts:
    :param issue_ts:
    :return:
    """
    # step1: 生成对应的站点 字典
    station_forecast = read_df_2_dict(df)
    forecast_dt: arrow.Arrow = arrow.get(forecast_ts)
    TIME_STEP: int = 1
    with session_yield_scope() as session:
        for key in station_forecast.keys():
            temp_forecastdata: List[StationForecastRealdataModel] = []
            temp_gp_id: int = 1
            temp_realdata: List[float] = station_forecast.get(key)
            try:
                for index, val in enumerate(temp_realdata):
                    temp_dt = forecast_dt.shift(hours=index * TIME_STEP)
                    temp_ts: int = temp_dt.int_timestamp
                    realdata_model = StationForecastRealdataModel(ty_code=ty_code, gp_id=temp_gp_id,
                                                                  station_code=key, timestamp=issue_ts,
                                                                  forecast_dt=temp_dt, forecast_ts=temp_ts,
                                                                  forecast_index=index, surge=val)
                    temp_forecastdata.append(realdata_model)
                session.add_all(temp_forecastdata)
                session.commit()
                print(f'写入code:{key}成功!')
            except Exception as ex:
                print(f'写入code:{key}失败!')

    pass


def get_path_files(read_path: str) -> List[pathlib.Path]:
    explorer = FileExplorer(read_path)
    files: List[pathlib.Path] = explorer.get_all_files()
    return files


def main():
    # TODO:[*] 25-05-29 注意此部分为测试数据，需要调整为动态获取参数
    ty_code: str = '2106'
    issue_ts: int = 1747125125
    """发布时间戳"""
    forecast_start_ts: int = 1725526800
    """预报起始时间戳"""
    files: List[pathlib.Path] = get_path_files(READPATH)
    for index, file in enumerate(files):
        file_schema = StationSurgeFileSchema(ty_code=ty_code, issue_ts=issue_ts, forecast_ts=forecast_start_ts,
                                             group_id=index, file_name=file.name)
        # shape:(40,72)
        temp_df: pd.DataFrame = read_dat_2_df(file)
        # 不需要转换
        temp_dict: dict = read_df_2_dict(temp_df)
        batch_df_2_db(temp_df, file_schema.ty_code, file_schema.issue_ts, file_schema.forecast_ts,
                      file_schema.group_path_type())

        pass

    pass


if __name__ == '__main__':
    main()
