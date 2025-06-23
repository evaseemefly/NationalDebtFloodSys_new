from conf.settings import db_pwd


class DBConfig:
    """
    DbConfig DB配置类
    :version: 1.4
    :date: 2020-02-11
    """

    driver = 'mysql+mysqldb'
    # 宿主机的mysql服务
    host = '172.16.30.163'
    port = '3306'
    username = 'surge'
    password = db_pwd
    database = 'sys_flood_nationaldebt'
    charset = 'utf8mb4'
    table_name_prefix = ''
    echo = False
    pool_size = 5  # 整数，连接池的大小，默认是 5。表示连接池中保持的连接数量。
    max_overflow = 10  # 整数，超过 pool_size 后可以额外创建的连接数。默认是 10。
    pool_timeout = 30  # 整数或浮点数，获取连接时的超时时间（秒），默认是 30 秒。
    pool_recycle = 60  # 整数，连接池中连接的回收时间（秒）。超过这个时间的连接会被自动断开并替换。默认是 -1，表示不使用回收。

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
