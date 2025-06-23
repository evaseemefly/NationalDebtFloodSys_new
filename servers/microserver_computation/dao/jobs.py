from typing import List, Optional, Any

from sqlalchemy import distinct, select

from common.default import MS_UNIT
from core.jobs import JobGenerateTyphoonPathFile, JobGenerateSurgeRasterPathFile
from dao.base import BaseDao
from models.models import TaskJobs
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathComplexSchema


class TaskDao(BaseDao):
    def submit_task(self, user_id: int, params: TyphoonPathComplexSchema):
        # 处理结束自动释放
        try:
            with self.session as session:
                timestamp = params.tyDetail.timeStamp / MS_UNIT
                job = JobGenerateTyphoonPathFile(user_id, params.tyDetail.tyCode, params.tyDetail.tyNameEn,
                                                 params.tyDetail.tyNameCh,
                                                 params.tyDetail.timeStamp)
                job.to_do(session)
                pass
        except Exception as ex:
            # TODO:[*] 25-05-07 Invalid argument(s) 'encoding' sent to create_engine(), using configuration MySQLDialect_mysqldb/QueuePool/Engine.  Please check that the keyword arguments are appropriate for this combination of components.
            # 在 create_engine 传入 encoding ，提示以上错误
            print(ex)
        pass
        pass

    def submit_surge_task(self, user_id: int, params: TyphoonPathComplexSchema):
        # 处理结束自动释放
        try:
            with self.session as session:
                timestamp = params.tyDetail.timeStamp / MS_UNIT
                job = JobGenerateSurgeRasterPathFile(user_id, params.tyDetail.tyCode, params.tyDetail.tyNameEn,
                                                     params.tyDetail.tyNameCh,
                                                     timestamp)
                job.to_do(session)
                pass
        except Exception as ex:
            # TODO:[*] 25-05-07 Invalid argument(s) 'encoding' sent to create_engine(), using configuration MySQLDialect_mysqldb/QueuePool/Engine.  Please check that the keyword arguments are appropriate for this combination of components.
            # 在 create_engine 传入 encoding ，提示以上错误
            print(ex)
        pass

    def get_task_list(self, ty_code: str) -> List[TyGroupTaskSchema]:
        """
            根据台风获取台风对应的任务集合
        @param ty_code:
        @return:
        """
        schemas: List[TyGroupTaskSchema] = []
        try:
            with self.session as session:
                stmt = select(TaskJobs).where(
                    TaskJobs.ty_code == ty_code).order_by(TaskJobs.issue_ts.desc())
                query: List[TaskJobs] = session.execute(stmt).scalars().all()
                # TODO:[-] 25-05-14 由于在dao层中查询结束就释放了 session 连接，此处将 orm -> pydantic model
                # return [TyGroupTaskSchema.from_orm(task) for task in query]
                for task in query:
                    # TODO:[-] 25-05-14 注意在 schema 中添加 orm_mode=True
                    #  ERROR: You must have the config attribute orm_mode=True to use from_orm
                    temp_schema = TyGroupTaskSchema.from_orm(task)
                    schemas.append(temp_schema)
                return schemas
            pass
        except Exception as ex:
            print(ex)
        pass
