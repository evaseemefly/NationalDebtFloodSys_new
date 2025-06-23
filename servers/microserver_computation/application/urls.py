# from controller import *
from controller.tasks import app as tasks_app
from controller.typhoon import app as ty_app
from controller.coverage import app as coverage_app
from controller.station import app as station_app
from controller.geo import app as geo_app

urlpatterns = [
    {"ApiRouter": tasks_app, "prefix": "/tasks", "tags": ["创建数值模式计算作业模块"]},
    {"ApiRouter": ty_app, "prefix": "/flood", "tags": ["台风模块"]},
    {"ApiRouter": coverage_app, "prefix": "/coverage", "tags": ["矢量文件模块"]},
    {"ApiRouter": station_app, "prefix": "/flood/station", "tags": ["站点模块"]},
    {"ApiRouter": geo_app, "prefix": "/geo", "tags": ["geo模块"]},
]
