from sqlalchemy import create_engine, Column, Integer, String, JSON, ForeignKey, Enum, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from common.enums import TaskStatus

Base = declarative_base()

class TaskJobs(Base):
    __tablename__ = 'task_jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_name = Column(String(255), nullable=False)
    parameters = Column(JSON, nullable=False)
    status = Column(Integer, default=TaskStatus.pending.value)
    duration = Column(Integer)
    gmt_submit_time = Column(DateTime, default=datetime.utcnow)
    gmt_completed_time = Column(DateTime)
    error_message = Column(Text)


class GeoCoverageFiles(Base):
    __tablename__ = 'geo_coverage_files'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pid = Column(Integer, nullable=False)
    is_del = Column(Integer, nullable=False)
    forecast_dt = Column(DateTime)
    forecast_ts = Column(Integer, nullable=False)
    issue_dt = Column(DateTime)
    issue_ts = Column(Integer)
    relative_path = Column(String(50), nullable=False)
    file_name = Column(String(100), nullable=False)
    file_ext = Column(String(50), nullable=False)
    coverage_type = Column(Integer, nullable=False)
    gmt_create_time = Column(DateTime, default=datetime.utcnow)
    gmt_modify_time = Column(DateTime)
    task_id = Column(Integer, ForeignKey('task_jobs.id'), nullable=False)


class AuthUser(Base):
    __tablename__ = 'auth_user'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    gmt_create_time = Column(DateTime, default=datetime.utcnow)
    gmt_modify_time = Column(DateTime, default=datetime.utcnow)


class AuthGroup(Base):
    __tablename__ = 'auth_group'

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(255), nullable=False)
    gmt_create_time = Column(DateTime, default=datetime.utcnow)
    gmt_modify_time = Column(DateTime, default=datetime.utcnow)


class RelaUserGroupFile(Base):
    __tablename__ = 'rela_user_group_file'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('auth_user.id'), nullable=False)
    group_id = Column(Integer, ForeignKey('auth_group.id'), nullable=False)
    file_id = Column(Integer, ForeignKey('geo_coverage_files.id'), nullable=False)
    file_type = Column(Integer)
    task_id = Column(Integer, ForeignKey('task_jobs.id'))