from datetime import datetime
from typing import List

from common.enums import TaskStatus
from common.exceptions import CoverageStoreError
from db.db import session_yield_scope
from models.models import AuthUser, AuthGroup, GeoCoverageFiles, TaskJobs, RelaUserGroupFile


def init_user():
    """
        初始化用户
    :return:
    """
    with session_yield_scope() as session:
        try:
            user1 = AuthUser(username='user1', email='user1@123.com')
            group1 = AuthGroup(group_name='group1')
            session.add(user1)
            session.add(group1)
            session.commit()
            pass
        except Exception as e:
            raise CoverageStoreError()


def batch2db(user_id: int, group_id: int):
    """
        批量向db写入实例
    :param user_id: user 表id
    :param group_id: group 表id
    :return:
    """

    """
        目前用户与群组使用固定的，不需要动态修改，只动态插入 geo_coverage_file 、 task_jobs 以及 rela_user_group_file
        具体步骤:
        step1: 创建 task
        step2: 将生成的nc(不生成就不需要),geotiff写入对应的db
        step3: 写入关系表 rela_user_group_file
        step4: 修改 task 的状态
    """
    with session_yield_scope() as session:
        try:
            # step1:
            task = TaskJobs(task_name='Example Task', parameters={'param1': 'value1'},
                            status=TaskStatus.in_progress.value)
            session.add(task)
            # step2: 向 geo_coverage_files 表中写入数据
            geo_file1 = GeoCoverageFiles(
                pid=1, is_del=0, forecast_dt=datetime.utcnow(), forecast_ts=1234567890,
                issue_dt=datetime.utcnow(), issue_ts=1234567890, relative_path='/path/to/file1',
                file_name='file1', file_ext='.txt', coverage_type=1, task_id=task.id
            )

            geo_file2 = GeoCoverageFiles(
                pid=2, is_del=0, forecast_dt=datetime.utcnow(), forecast_ts=1234567890,
                issue_dt=datetime.utcnow(), issue_ts=1234567890, relative_path='/path/to/file2',
                file_name='file2', file_ext='.txt', coverage_type=1, task_id=task.id
            )
            # TODO:[-] 此处我改写为循环，你参考此处即可 将geotiff批量写入db
            files: List[GeoCoverageFiles] = [geo_file1, geo_file2]
            for temp_file in files:
                session.add(temp_file)

            # step3: 写入关系表 并批量更新参考上面 todo
            relation1 = RelaUserGroupFile(user_id=user_id, group_id=group_id, file_id=geo_file1.id, task_id=task.id)
            relation2 = RelaUserGroupFile(user_id=user_id, group_id=group_id, file_id=geo_file2.id, task_id=task.id)
            relas: List[RelaUserGroupFile] = [relation1, relation2]
            for temp_rela in relas:
                session.add(temp_rela)

            # step4: 更新 task的状态
            task.status = TaskStatus.completed.value
            session.update(task)
            session.commit()

            pass
        except Exception as e:

            raise CoverageStoreError()


def main():
    # 已经初始化创建测试用户及群组不需要再次执行
    # init_user()
    default_user_id: int = 1
    default_group_id: int = 1
    batch2db(default_user_id, default_group_id)

    pass


if __name__ == '__main__':
    main()
