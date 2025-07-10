from celery import Celery
from config.consul_config import consul_config

# --- Celery 配置 ---
# Broker 和 Backend 连接到 Docker Compose 中的 'redis' 服务
# 在容器网络中，服务名 'redis' 会被解析为 Redis 容器的 IP
# BROKER_URL = 'redis://redis:6379/0'
# RESULT_BACKEND_URL = 'redis://redis:6379/1'

CONSUL_REDIS_AGENT = consul_config.get_consul_kv('redis_agent')
flood_agent = CONSUL_REDIS_AGENT['FLOOD_AGENT']
# 'redis://0.0.0.0:6379/0'
BROKER_URL = flood_agent.get('BROKER_URL')
# +++ 直接硬编码进行测试 +++
# BROKER_URL = 'redis://128.5.10.21:6379/0'

# 'redis://0.0.0.0:6379/1'
# 修改为 21 gpu 集群：http://128.5.10.21/
# 'redis://128.5.10.21:6379/0'
RESULT_BACKEND_URL = flood_agent.get('RESULT_BACKEND_URL')

print("="*80)
print(f"DEBUG: Preparing to initialize Celery app.")
print(f"DEBUG: BROKER_URL fetched from Consul is: '{BROKER_URL}'")
print(f"DEBUG: The type of BROKER_URL is: {type(BROKER_URL)}")
print(f"DEBUG: RESULT_BACKEND_URL fetched from Consul is: '{RESULT_BACKEND_URL}'")
print(f"DEBUG: The type of RESULT_BACKEND_URL is: {type(RESULT_BACKEND_URL)}")
print("="*80)

# 强制检查，如果 BROKER_URL 是 None、空字符串或其他无效值，就立即报错
if not BROKER_URL or not isinstance(BROKER_URL, str):
    raise ValueError(f"CRITICAL FAILURE: The BROKER_URL is invalid. Value: '{BROKER_URL}'. Cannot create Celery app.")

# 任务序列化和反序列化使用JSON方案
CELERY_TASK_SERIALIZER = 'pickle'
# 读取任务结果使用JSON
CELERY_RESULT_SERIALIZER = 'json'
# 任务过期时间，不建议直接写86400，应该让这样的magic数字表述更明显
CELERY_TASK_RESULT_EXPIRES = 60 * 60 * 24
# 指定接受的内容类型，是个数组，可以写多个
CELERY_ACCEPT_CONTENT = ['json', 'pickle']

# 注意：这里只定义了 Celery app 的配置，并不包含任务的实现
# 真正的任务实现在 host_worker/worker.py 中
# celery_app = Celery(
#     'tasks',  # 名字可以和 worker 的不同，但 task name 必须一致
#     broker=BROKER_URL,
#     backend=RESULT_BACKEND_URL
# )

celery_app = Celery(
    'tasks',
    broker=BROKER_URL,
    backend=RESULT_BACKEND_URL
)
celery_app.conf.update(
    CELERY_TASK_SERIALIZER=CELERY_TASK_SERIALIZER,
    CELERY_RESULT_SERIALIZER=CELERY_RESULT_SERIALIZER,
    CELERY_IGNORE_RESULT=True,
    CELERYD_PREFETCH_MULTIPLIER=10,
    CELERYD_MAX_TASKS_PER_CHILD=200,
    CELERY_ACCEPT_CONTENT=CELERY_ACCEPT_CONTENT,
    # 由于出现了超时的情况
    BROKER_TRANSPORT_OPTIONS={'visibility_timeout': 60 * 60, 'retry_policy': {
        'timeout': 60 * 60.0
    }},  # 单位为 s
    CELERY_EVENT_QUEUE_EXPIRES=60 * 60,
    CELERY_EVENT_QUEUE_TTL=60 * 60,
    CELERYD_CANCEL_LONG_RUNNING_TASKS_ON_CONNECTION_LOSS=True,
)
