from typing import List, Optional, Any

from sqlalchemy import distinct, select

from common.default import MS_UNIT
from common.enums import TyphoonGroupEnum
from core.jobs import JobGenerateTyphoonPathFile, JobGenerateSurgeRasterPathFile
from dao.base import BaseDao
from models.models import TyphoonForecastGrouppath, TyphoonForecastRealdata
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathComplexSchema, TyphoonDistGroupSchema, TyphoonPointSchema


class TyphoonDao(BaseDao):

    def get_group_list(self, ty_code: str) -> List[TyphoonDistGroupSchema]:
        """
            根据台风获取台风对应的任务集合
        @param ty_code:
        @return:
        """
        schemas: List[TyphoonDistGroupSchema] = []
        try:
            with self.session as session:
                stmt = select(TyphoonForecastGrouppath.timestamp).distinct().where(
                    TyphoonForecastGrouppath.ty_code == ty_code)
                query: List[TyphoonForecastGrouppath] = session.execute(stmt).scalars().all()
                for temp in query:
                    # TODO:[-] 25-05-14 注意在 schema 中添加 orm_mode=True
                    #  ERROR: You must have the config attribute orm_mode=True to use from_orm
                    temp_schema = TyphoonDistGroupSchema(tyCode=ty_code, timestamp=temp)
                    schemas.append(temp_schema)
                return schemas
            pass
        except Exception as ex:
            print(ex)
        pass

    def get_grouppath_list(self, ty_code: str, issue_ts: int) -> List[
        TyphoonPathComplexSchema]:
        """
            step1: code,issue_ts -> grouppath 集合
            step2: foreach grouppath -> 对应 ty_realdata 集合
            step3: 组合成 schema 返回
        @param ty_code:
        @param issue_ts:
        @return:
        """
        try:
            with self.session as session:
                # step1:
                groups_stmt = select(TyphoonForecastGrouppath).where(TyphoonForecastGrouppath.ty_code == ty_code,
                                                                     TyphoonForecastGrouppath.timestamp == issue_ts)
                group_query: List[TyphoonForecastGrouppath] = session.execute(groups_stmt).scalars().all()
                list_group_schema: List[TyphoonPathComplexSchema] = []
                # step2:
                for temp_group in group_query:
                    temp_group_type = temp_group.ty_path_type
                    temp_group_id: int = temp_group.id
                    temp_group_path_list: List[TyphoonPointSchema] = []
                    temp_path_stmt = select(TyphoonForecastRealdata).where(
                        TyphoonForecastRealdata.gp_id == temp_group_id)
                    temp_path_query: List[TyphoonForecastRealdata] = session.execute(temp_path_stmt).scalars().all()
                    for temp_path in temp_path_query:
                        temp_path_schema: TyphoonPointSchema = TyphoonPointSchema(lat=temp_path.lat, lon=temp_path.lon,
                                                                                  bp=temp_path.bp,
                                                                                  forecastDt=temp_path.forecast_dt,
                                                                                  isForecast=True,
                                                                                  tyType=temp_group.ty_path_type)
                        temp_group_path_list.append(temp_path_schema)
                    temp_group_schema: TyphoonPathComplexSchema = TyphoonPathComplexSchema(tyCode=ty_code,
                                                                                           issueTs=temp_group.timestamp,
                                                                                           groupType=temp_group.ty_path_type,
                                                                                           tyPathList=temp_group_path_list)
                    list_group_schema.append(temp_group_schema)
                return list_group_schema
                pass
            pass
        except Exception as ex:
            print(ex)
        pass

    def get_dist_grouppath_list(self, ty_code: str, issue_ts: int) -> List[TyphoonPathComplexSchema]:
        """
            获取指定台风案例创建的5个集合路径
        @param ty_code:
        @param issue_ts:
        @return:
        """
        list_group_schema: List[TyphoonPathComplexSchema] = []
        try:
            with self.session as session:
                # step1:
                groups_stmt = select(TyphoonForecastGrouppath).where(TyphoonForecastGrouppath.ty_code == ty_code,
                                                                     TyphoonForecastGrouppath.timestamp == issue_ts)
                group_query: List[TyphoonForecastGrouppath] = session.execute(groups_stmt).scalars().all()
                list_group_schema = [
                    TyphoonPathComplexSchema(tyCode=temp.ty_code, issueTs=issue_ts, groupType=temp.ty_path_type) for
                    temp in group_query]
            pass
        except Exception as ex:
            print(ex)
        return list_group_schema
