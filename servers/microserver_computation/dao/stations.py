from typing import List

from sqlalchemy import select

from dao.base import BaseDao
from models.models import StationInfo
from schema.stations import StionInfoSchema


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
                schemas = [StionInfoSchema.from_orm(temp) for temp in query]
        except Exception as ex:
            print(ex)

        return schemas
