from arrow import Arrow
from sqlalchemy import String, Float, Boolean, Integer, DateTime, ForeignKey, func, Text, Index, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.mysql import JSON
from geoalchemy2 import Geometry
from datetime import datetime
from typing import Optional, Any, List

from commons.default import DEFAULT_CODE
from commons.enums import FloodLevelEnum, TaskStatusEnum
from models.base_model import IDel, IIdIntModel, IForecastTime, IIssueTime, IModel


class Base(DeclarativeBase):
    pass


class Station(Base):
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

    def __repr__(self) -> str:
        return f"Station(id={self.id}, station_name={self.station_name}, station_code={self.station_code})"


class StationForecastRealdataModel(Base):
    """
        站点预报增水——需要加入用户名——按用户名分表存储
    """
    __tablename__ = "station_forecast_realdata_user1"
    __table_args__ = {"schema": "sys_flood_nationaldebt"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    is_del: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    ty_code: Mapped[str] = mapped_column(String(200), nullable=False, comment="台风编号")
    gp_id: Mapped[int] = mapped_column(Integer, nullable=False, comment="group id", default=-1)
    station_code: Mapped[str] = mapped_column(String(200), nullable=False, comment="站代号")
    forecast_dt: Mapped[Optional[datetime]] = mapped_column(DateTime(2), comment="预报时间(utc)")
    forecast_ts: Mapped[int] = mapped_column(Integer, comment="预报时间戳")
    forecast_index: Mapped[int] = mapped_column(Integer, nullable=False)
    surge: Mapped[float] = mapped_column(Float, nullable=False, comment="预报增水")
    issue_time: Mapped[str] = mapped_column(Integer, comment="发布时间")
    gmt_create_time: Mapped[Optional[datetime]] = mapped_column(DateTime(6), default=datetime.utcnow())
    gmt_modify_time: Mapped[Optional[datetime]] = mapped_column(DateTime(6), default=datetime.utcnow())
    grouppath_type: Mapped[int] = mapped_column(Integer, nullable=False, comment="集合路径枚举", default=-1)

    def __repr__(self) -> str:
        return (f"StationForecastRealdata(id={self.id}, ty_code={self.ty_code}, "
                f"station_code={self.station_code}, forecast_dt={self.forecast_dt})")


class StationAstronomicTide(Base):
    """
    天文潮站点数据表 ORM 模型
    """
    __tablename__ = 'station_astronomic_tide'

    # 指定表所在的 schema
    __table_args__ = ({'schema': 'sys_flood_nationaldebt'},)

    # --- 字段定义 ---

    # id: Mapped[int] 表示该字段在 Python 中是 int 类型
    # mapped_column(...) 用于定义数据库中的列属性
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, comment="主键ID")

    # 对于可空字段, Python 类型注解为 Optional[<type>] 或 <type> | None
    is_del: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    gmt_create_time: Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.utcnow(),
                                                                comment="记录创建时间")

    gmt_modify_time: Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.utcnow(),
                                                                comment="记录修改时间")

    station_code: Mapped[Optional[str]] = mapped_column(String(20), comment="站点代码")

    gmt_realtime: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="数据实际时间")

    tide: Mapped[Optional[float]] = mapped_column(Float, comment="天文潮位值")

    ts: Mapped[Optional[int]] = mapped_column(Integer, comment="UNIX时间戳")

    def __repr__(self) -> str:
        """
        提供一个清晰的字符串表示形式，便于调试
        """
        return (
            f"<StationAstronomicTide(id={self.id}, "
            f"station_code='{self.station_code}', "
            f"gmt_realtime={self.gmt_realtime})>"
        )


# class RelaUserStationForecast(Base):
#     """用户与气象站预报数据关联表"""
#     __tablename__ = 'rela_user_station_forecast'
#
#     id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
#     user_id = Column(String(64), nullable=False, comment='用户ID')
#     station_forecast_table_name = Column(String(100), nullable=False, comment='气象站预报数据表名')
#     relation_type = Column(Integer, default=1, comment='关联类型：1-订阅，2-收藏，3-管理')
#     status = Column(Integer, default=1, comment='状态：0-禁用，1-启用')
#     priority = Column(Integer, default=0, comment='优先级，数值越大优先级越高')
#     gmt_create_time = Column(DateTime, default=datetime.now, comment='创建时间')
#     gmt_update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
#     remark = Column(Text, comment='备注信息')


class GeoFloodLevelPolygon(Base):
    __tablename__ = "geo_floodlevel_polygon"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[float] = mapped_column(Float, comment="增水值")
    ty_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="台风编号")
    name: Mapped[Optional[str]] = mapped_column(String(100), comment="多边形名称")
    description: Mapped[Optional[str]] = mapped_column(Text, comment="描述信息")
    properties: Mapped[Optional[str]] = mapped_column(Text, comment="GeoJSON properties 的 JSON 字符串")
    geom: Mapped[Any] = mapped_column(Geometry("POLYGON", srid=4326), nullable=False, comment="多边形几何数据")
    flood_level: Mapped[int] = mapped_column(Integer, comment="淹没等级——枚举", default=FloodLevelEnum.GTE100.value)
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


class GeoPolygon(Base):
    """存储 GeoJSON 多边形数据的模型"""

    __tablename__ = "geo_polygons"
    __table_args__ = {"extend_existing": True}  # 允许扩展已定义的表

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[float] = mapped_column(Float, comment="增水值")
    ty_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="台风编号")
    # gp_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True, comment="多边形标识ID")
    name: Mapped[Optional[str]] = mapped_column(String(100), comment="多边形名称")
    description: Mapped[Optional[str]] = mapped_column(Text, comment="描述信息")
    properties: Mapped[Optional[str]] = mapped_column(Text, comment="GeoJSON properties 的 JSON 字符串")
    geom: Mapped[Any] = mapped_column(Geometry("POLYGON", srid=4326), nullable=False, comment="多边形几何数据")
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
    # __table_args__ = (
    #     Index("idx_created_at", "gmt_create_time"),
    #     Index("idx_updated_at", "gmt_update_time"),
    #     {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_unicode_ci"}
    # )

    def __repr__(self) -> str:
        return f"<GeoPolygon(id={self.id}, ty_code='{self.ty_code}', gp_id='{self.gp_id}')>"


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


class ICoverageFileModel:
    pass


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
