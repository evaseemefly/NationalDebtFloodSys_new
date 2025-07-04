# from config.consul_config import CONSUL_OPTIONS
from urllib.parse import quote_plus

from config.consul_config import consul_config

# from util.consul import ConsulConfigClient

# CONSUL_HOST: str = CONSUL_OPTIONS.get('SERVER').get('HOST')
# CONSUL_PORT: int = CONSUL_OPTIONS.get('SERVER').get('PORT')
# consul_config = ConsulConfigClient(CONSUL_HOST, CONSUL_PORT)

# 温带风暴潮数据库配置
# 本地测试
# CONSUL_DB_CONFIG = consul_config.get_consul_kv('wd_db_config_local')
CONSUL_DB_CONFIG = consul_config.get_consul_kv('db_national')


class DBConfig:
    """
    DbConfig DB配置类
    :version: 1.4
    :date: 2020-02-11
    TODO:[-] 23-06-28 此处修改为通过 consul 统一获取配置信息
    """

    # TODO:[*] 25-06-23 由于mac上mysql client客户端一直有问题，当前使用的driver为："driver" : "mysql+mysqldb",
    # 修改为 pymysql
    # driver = CONSUL_DB_CONFIG.get('driver')
    driver = 'mysql+pymysql'
    host = CONSUL_DB_CONFIG.get('host')
    # 宿主机的mysql服务
    # host = 'host.docker.internal'
    port = CONSUL_DB_CONFIG.get('port')
    username = CONSUL_DB_CONFIG.get('username')
    password = quote_plus(CONSUL_DB_CONFIG.get('password'))
    database = CONSUL_DB_CONFIG.get('database')
    charset = CONSUL_DB_CONFIG.get('charset')
    table_name_prefix = ''
    echo = CONSUL_DB_CONFIG.get('echo')
    pool_size = CONSUL_DB_CONFIG.get('pool_size')
    max_overflow = CONSUL_DB_CONFIG.get('max_overflow')
    pool_recycle = CONSUL_DB_CONFIG.get('pool_recycle')

    def get_url(self):
        config = [
            self.driver,
            '://',
            self.username,
            ':',
            self.password,
            '@',
            self.host,
            ':',
            self.port,
            '/',
            self.database,
            '?charset=',
            self.charset,
        ]

        return ''.join(config)
