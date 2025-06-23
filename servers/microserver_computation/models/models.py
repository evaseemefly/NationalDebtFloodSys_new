from typing import Optional, List, Any

from arrow import Arrow
from sqlalchemy import create_engine, Column, Float, Integer, String, JSON, ForeignKey, Enum, DateTime, Text, Boolean, \
    UniqueConstraint, func, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from sqlalchemy.orm import mapped_column, Mapped, relationship
from geoalchemy2 import Geometry

from common.default import DEFAULT_PATH, DEFAULT_NAME, DEFAULT_ENUM, NONE_ID, DEFAULT_CODE
from common.enums import TaskStatusEnum, FloodAreaLevelEnum
from models.base_model import IDel, IForecastTime, IModel, IIdIntModel, IReleaseTime, IIssueTime

Base = declarative_base()


class TaskJobs(Base):
    __tablename__ = 'task_jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    parameters = Column(JSON, nullable=False)
    status = Column(Integer, default=TaskStatusEnum.pending.value)
    duration = Column(Integer)
    gmt_submit_time = Column(DateTime, default=datetime.utcnow)
    gmt_completed_time = Column(DateTime)
    error_message = Column(Text)
    ty_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)
    issue_ts: Mapped[int] = mapped_column(default=0)

    # 添加这个关系定义
    group_paths: Mapped[List["RelaGroupPathTask"]] = relationship("RelaGroupPathTask", back_populates="task")


class ICoverageFileModel(Base):
    __abstract__ = True
    relative_path: Mapped[str] = mapped_column(String(50), default=DEFAULT_PATH)
    file_name: Mapped[str] = mapped_column(String(100), default=DEFAULT_NAME)


class GeoCoverageFiles(IDel, IIdIntModel, ICoverageFileModel, IForecastTime, IIssueTime, IModel):
    """

    """
    __tablename__ = 'geo_coverage_files'

    coverage_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    """预报产品类型"""

    group_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    """group path 路径枚举对应的value"""
    # group_id: Mapped[int] = mapped_column(default=NONE_ID)
    # """对应grouppath表的id"""
    task_id: Mapped[int] = mapped_column(default=NONE_ID)
    ty_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)


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


class RelaTaskFiles(Base):
    __tablename__ = 'rela_task_files'
    __table_args__ = {'schema': 'sys_flood_nationaldebt'}

    # 主键
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 外键字段
    file_id: Mapped[int] = mapped_column(ForeignKey('geo_coverage_files.id'), nullable=False)
    file_type: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    task_id: Mapped[Optional[int]] = mapped_column(ForeignKey('task_jobs.id'), nullable=True)

    # 关系定义
    file: Mapped["GeoCoverageFiles"] = relationship("GeoCoverageFiles")
    task: Mapped[Optional["TaskJobs"]] = relationship("TaskJobs")


class RelaGroupPathTask(Base):
    """
        Foreign key associated with column 'rela_grouppath_task.
        group_id' could not find table 'typhoon_forecast_grouppath' with which to generate a foreign key to target column 'id'
    """
    __tablename__ = 'rela_grouppath_task'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(
        ForeignKey('task_jobs.id', ondelete='CASCADE'),
        nullable=False,
        index=True
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey('sys_flood_nationaldebt.typhoon_forecast_grouppath.id', ondelete='CASCADE'),
        nullable=False,
        index=True
    )

    # 关系定义
    # TODO:[*] 25-05-12
    #  Mapper 'Mapper[TaskJobs(task_jobs)]' has no property 'group_paths'.
    #  If this property was indicated from other mappers or configure events,
    #  ensure registry.configure() has been called.
    task: Mapped["TaskJobs"] = relationship("TaskJobs", back_populates="group_paths")
    group: Mapped["TyphoonForecastGrouppath"] = relationship("TyphoonForecastGrouppath", back_populates="task_paths")

    # 添加复合唯一约束，防止重复关联
    __table_args__ = (
        UniqueConstraint('task_id', 'group_id', name='uq_task_group'),
    )


class TyphoonForecastDetailinfo(Base):
    """台风预报详细信息表"""
    __tablename__ = 'typhoon_forecast_detailinfo'
    __table_args__ = {'schema': 'sys_flood_nationaldebt'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    is_del = Column(Boolean, default=False)
    gmt_create_time = Column(DateTime(6), default=datetime.utcnow)
    gmt_modify_time = Column(DateTime(6), default=datetime.utcnow)
    code = Column(String(200), nullable=False)
    name_ch = Column(String(100), nullable=False)
    name_en = Column(String(100), nullable=False)
    gmt_start = Column(DateTime(6), nullable=True)
    gmt_end = Column(DateTime(6), nullable=True)
    forecast_source = Column(Integer, nullable=False)
    is_forecast = Column(Boolean, nullable=False, default=True)
    timestamp: Mapped[int] = mapped_column(default=0)

    def __repr__(self):
        return f"<TyphoonForecastDetailinfo(id={self.id}, code={self.code})>"


class TyphoonForecastGrouppath(Base):
    """台风预报路径组表"""
    __tablename__ = 'typhoon_forecast_grouppath'
    __table_args__ = {'schema': 'sys_flood_nationaldebt'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    is_del = Column(Boolean, nullable=False, default=False)
    gmt_create_time = Column(DateTime(6), default=datetime.utcnow)
    gmt_modify_time = Column(DateTime(6), default=datetime.utcnow)
    ty_id = Column(Integer, nullable=False)
    ty_code = Column(String(200), nullable=False)
    file_name = Column(String(200), nullable=False)
    relative_path = Column(String(500), nullable=False)
    timestamp: Mapped[int] = mapped_column(default=0)
    ty_path_type = Column(String(3), nullable=False)

    # 添加这个关系定义
    task_paths: Mapped[List["RelaGroupPathTask"]] = relationship("RelaGroupPathTask", back_populates="group")

    def __repr__(self):
        return f"<TyphoonForecastGrouppath(id={self.id}, ty_code={self.ty_code})>"


class TyphoonForecastRealdata(Base):
    """台风预报实际数据表"""
    __tablename__ = 'typhoon_forecast_realdata'
    __table_args__ = {'schema': 'sys_flood_nationaldebt'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    is_del = Column(Boolean, nullable=False, default=False)
    gmt_create_time = Column(DateTime(6), default=datetime.utcnow)
    gmt_modify_time = Column(DateTime(6), default=datetime.utcnow)
    ty_id = Column(Integer, nullable=False)
    gp_id = Column(Integer, nullable=False)
    forecast_dt = Column(DateTime(6), nullable=True)
    forecast_index = Column(Integer, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    bp = Column(Float, nullable=False)
    gale_radius = Column(Float, nullable=False, default=-1)
    timestamp: Mapped[int] = mapped_column(default=0)

    def __repr__(self):
        return f"<TyphoonForecastRealdata(id={self.id}, ty_id={self.ty_id})>"


class StationInfo(Base):
    __tablename__ = "station_info"
    __table_args__ = {"schema": "sys_flood_nationaldebt"}

    station_name: Mapped[Optional[str]] = mapped_column(String(10))
    station_code: Mapped[Optional[str]] = mapped_column(String(10))
    lat: Mapped[Optional[float]] = mapped_column(Float)
    lon: Mapped[Optional[float]] = mapped_column(Float)
    desc: Mapped[Optional[str]] = mapped_column(String(500))
    is_abs: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pid: Mapped[Optional[int]] = mapped_column(Integer)
    is_in_common_use: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    sort: Mapped[Optional[int]] = mapped_column(Integer)
    rid: Mapped[Optional[int]] = mapped_column(Integer)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    is_del: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    gmt_create_time: Mapped[Optional[datetime]] = mapped_column(DateTime(6))
    gmt_modify_time: Mapped[Optional[datetime]] = mapped_column(DateTime(6))


class GeoPolygon(Base):
    """存储 GeoJSON 多边形数据的模型"""

    __tablename__ = "geo_polygons"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[float] = mapped_column(Float, comment="增水值")
    ty_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="台风编号")
    name: Mapped[Optional[str]] = mapped_column(String(100), comment="多边形名称")
    description: Mapped[Optional[str]] = mapped_column(Text, comment="描述信息")
    properties: Mapped[Optional[str]] = mapped_column(Text, comment="GeoJSON properties 的 JSON 字符串")
    geom: Mapped[Any] = mapped_column(Geometry("POLYGON", srid=4326), nullable=False, comment="多边形几何数据")
    # geom: Mapped[Any] = mapped_column(Geometry("POLYGON", srid=4326, spatial_index=True,
    #                                            from_text='ST_GeomFromText',
    #                                            name='mysql'),
    #                                   nullable=False,
    #                                   comment="多边形几何数据")
    gmt_create_time: Mapped[datetime] = mapped_column(
        default=datetime.utcnow(),
        comment="创建时间"
    )
    gmt_update_time: Mapped[datetime] = mapped_column(
        default=datetime.utcnow(),
        onupdate=func.current_timestamp(),
        comment="更新时间"
    )

    issue_time: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp,
                                            comment="发布时间")

    # 定义索引
    __table_args__ = (
        Index("idx_created_at", "gmt_create_time"),
        Index("idx_updated_at", "gmt_update_time"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_unicode_ci"}
    )

    def __repr__(self) -> str:
        return f"<GeoPolygon(id={self.id}, ty_code='{self.ty_code}', gp_id='{self.gp_id}')>"


class GeoFloodLevelPolygon(Base):
    """
        淹没等级多边形
    """
    __tablename__ = "geo_floodlevel_polygon"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[float] = mapped_column(Float, comment="增水值")
    ty_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="台风编号")
    name: Mapped[Optional[str]] = mapped_column(String(100), comment="多边形名称")
    description: Mapped[Optional[str]] = mapped_column(Text, comment="描述信息")
    properties: Mapped[Optional[str]] = mapped_column(Text, comment="GeoJSON properties 的 JSON 字符串")
    geom: Mapped[Any] = mapped_column(Geometry("POLYGON", srid=4326), nullable=False, comment="多边形几何数据")
    flood_level: Mapped[int] = mapped_column(Integer, comment="淹没等级——枚举", default=FloodAreaLevelEnum.GTE100.value)
    gmt_create_time: Mapped[datetime] = mapped_column(
        default=datetime.utcnow(),
        comment="创建时间"
    )
    gmt_update_time: Mapped[datetime] = mapped_column(
        default=datetime.utcnow(),
        onupdate=func.current_timestamp(),
        comment="更新时间"
    )

    issue_time: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp,
                                            comment="发布时间")

    def __repr__(self) -> str:
        return f"<GeoFloodLevelPolygon(id={self.id}, ty_code='{self.ty_code}', gp_id='{self.gp_id}')>"
