import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


class CeleryConfig:
    """
    Celery 配置
    """
    #
    CELERY_BROKER_URL = 'redis://localhost:6379/0'
    CELERY_RESULT_BACKEND = 'redis://localhost:6379/1'


class BaseConfig:
    # 自定义配置
    SCRIPTS_DIR = os.path.join(BASE_DIR, 'scripts')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'output')


class Config:
    # 数据库配置
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(BASE_DIR, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False


celery_setting = CeleryConfig()
'''celery配置'''

base_setting = BaseConfig()
'''基础配置'''
