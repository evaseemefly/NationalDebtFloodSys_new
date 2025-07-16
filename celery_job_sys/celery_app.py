from celery import Celery
from config.config import celery_setting

# 创建 Celery 实例
# 'job_system' 是当前项目的名字
# include 参数告诉 Celery 去哪里发现任务
