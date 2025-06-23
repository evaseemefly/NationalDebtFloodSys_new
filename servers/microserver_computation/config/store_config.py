STORE_CONFIG = {
    'STORE_ROOT_PATH': r'E:/05DATA/99test/TYPHOON/data'
}
"""
    STORE_ROOT_PATH : 存储的根目录
"""


class StoreConfig:
    # ip = 'http://192.168.0.109:82'
    ip = 'http://localhost:82'

    store_relative_path: str = 'images/GD_FLOOD'

    @classmethod
    def get_ip(cls):
        """
            获取存储动态ip
        @return:
        """
        return cls.ip

    @classmethod
    def get_store_relative_path(cls):
        return cls.store_relative_path
