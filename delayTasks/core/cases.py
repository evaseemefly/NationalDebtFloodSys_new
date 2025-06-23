# 几个相关案例
# 移植自台风集合预报路径系统，数据库数据也和此项目数据库采用相同设计
from typing import List, Union

from models.mid_models import TyphoonForecastDetailMidModel, GroupTyphoonPathMidModel


class TySpiderCase:
    """
        台风爬虫 case
    """

    def __init__(self, code: str):
        self.code = code

    def spider_get_ty(self, ty_code: str) -> List[Union[TyphoonForecastDetailMidModel, List[GroupTyphoonPathMidModel]]]:
        """
            TODO:[*] 25-04-23 需要接入对应的获取代码
            根据 台风编号获取对应的台风路径 返回爬取到的台风路径及对应的基础信息
        :param ty_code: 台风编号(str)
        :return: [台风基础信息,对应台风路径集合]
        """

        ty_source = TyphoonForecastDetailMidModel()
        """原始台风信息"""

        ty_path_list: List[GroupTyphoonPathMidModel] = []
        """原始台风的路径信息"""

        return [ty_source, ty_path_list]

    def batch2db(self, ty_code: str) -> int:
        """
            根据 ty_code 获取最新的路径
        :param ty_code:
        :return:
        """
