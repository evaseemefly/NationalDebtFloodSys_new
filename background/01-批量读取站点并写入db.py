import pandas as pd
import numpy as np
import pathlib

from db_factory import session_yield_scope
from models import Station

READPATH: str = r'E:\01data\99test\station_info_latlon.csv'


def batch_read_station_infos(read_path: str):
    if pathlib.Path(read_path).exists():
        df = pd.read_csv(read_path)
        # 使用会话保存到数据库
        with session_yield_scope() as session:
            count: int = len(df)
            for i in range(0, count):
                temp_row = df.iloc[i]
                temp_name = temp_row['staNameCn']
                temp_code = temp_row['staNameEn']
                temp_lat = temp_row['staLat']
                temp_lon = temp_row['staLon']
                temp_model = Station(station_name=temp_name, station_code=temp_code, lat=temp_lat, lon=temp_lon,
                                     desc='', is_in_common_use=True, sort=999, rid=-1)
                session.add(temp_model)
                # session.add(new_station)

            session.commit()

    pass


def main():
    batch_read_station_infos(READPATH)


if __name__ == '__main__':
    main()
