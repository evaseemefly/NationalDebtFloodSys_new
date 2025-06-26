from typing import List

from sqlalchemy import select

from common.enums import TyphoonGroupEnum
from dao.base import BaseDao
from models.models import StationInfo, StationForecastRealdataModel
from schema.stations import StionInfoSchema, StationSurgeSchema, StationGroupSurgeSchema


class StationDao(BaseDao):

    def get_all_stations(self) -> List[StionInfoSchema]:
        """
            获取所有站点的信息
        @return:
        """
        schemas: List[StionInfoSchema] = []
        try:
            with self.session as session:
                stmt = select(StationInfo).where(
                    StationInfo.is_del == False)
                query: List[StationInfo] = session.execute(stmt).scalars().all()
                # schemas = [StionInfoSchema.from_orm(temp) for temp in query]
                schemas = [StionInfoSchema.model_validate(temp) for temp in query]
        except Exception as ex:
            print(ex)

        return schemas

    def get_station_groupsurge(self, station_code: str, ty_code: str, issue_ts: int, grouppath: TyphoonGroupEnum) -> \
            List[
                StationGroupSurgeSchema]:
        """
            根据参数获取对应的集合
        @param station_code:
        @param ty_code:
        @param grouppath:
        @return:
        """
        # step1: 获取查询参数对应结果的 distinct group path
        # step2: 根据每个 group path 查询对应的增水，并按照降序排列，生成数组
        # step3: 创建返回schema集合
        # step4: 返回
        try:
            with self.session as session:
                group_stmt = select(StationForecastRealdataModel.grouppath_type).where(
                    StationForecastRealdataModel.is_del == False,
                    StationForecastRealdataModel.station_code == station_code,
                    StationForecastRealdataModel.ty_code == ty_code,
                    StationForecastRealdataModel.issue_time == issue_ts).distinct(
                    StationForecastRealdataModel.grouppath_type)
                grouppath: List[int] = session.execute(group_stmt).scalars().all()
                # 尝试使用 group by grouppath 进行分组查询
                res_schema: List[StationGroupSurgeSchema] = []
                for temp_group in grouppath:
                    stmt = select(StationForecastRealdataModel).where(
                        StationForecastRealdataModel.is_del == False,
                        StationForecastRealdataModel.station_code == station_code,
                        StationForecastRealdataModel.ty_code == ty_code,
                        StationForecastRealdataModel.issue_time == issue_ts,
                        StationForecastRealdataModel.grouppath_type == temp_group).order_by(
                        StationForecastRealdataModel.forecast_ts)
                    query: List[StationForecastRealdataModel] = session.execute(stmt).scalars().all()
                    temp_surge_res = [StationSurgeSchema.model_validate(temp) for temp in query]
                    temp_res = StationGroupSurgeSchema(group_type=temp_group, surge_list=temp_surge_res)
                    res_schema.append(temp_res)
                return res_schema
        except Exception as ex:
            print(ex)
        pass
