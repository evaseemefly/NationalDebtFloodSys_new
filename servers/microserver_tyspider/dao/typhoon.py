import http
import http.client
import json
from datetime import datetime, timedelta

# from arrow import arrow
import arrow

from dao.base import BaseDao, get_agent
from models.mid_models import TyForecastRealDataMidModel, TyPathMidModel, TyDetailMidModel


class TyphoonSpiderDao(BaseDao):
    def _get_year(self, ty_code: str) -> str:
        """
            根据输入的台风编号获取对应的年份
        :param ty_code:
        :return:
        """
        year = f'20{ty_code[:2]}'
        return year

    def check_ty_exist(self, ty_code: str):
        """
                   测试抓取台风网的数据
               :return:
               """
        baseUrl = 'typhoon.nmc.cn'
        ty_obj: TyDetailMidModel = None
        # -- 方式3 --
        # 参考文章: https://blog.csdn.net/xietansheng/article/details/115557974
        # 出现了被屏蔽的问题
        headers = {
            'User-Agent': get_agent()}

        conn = http.client.HTTPConnection(baseUrl)
        ty_year = self._get_year(ty_code)
        # conn.request('GET', f'/weatherservice/typhoon/jsons/list_{ty_year}', headers=headers)
        conn.request('GET', f'/weatherservice/typhoon/jsons/list_{ty_year}',
                     headers=headers)
        res = conn.getresponse()
        content = res.read().decode('utf-8')
        '''
            typhoon_jsons_list_default(({ "typhoonList":[[2723.....]
                                        }))
        '''
        '''
            { "typhoonList":[[2723.....]
                                        }
        '''
        new_json = '{' + content[25: -2] + '}'

        # print(content)
        obj = json.loads(new_json, strict=False)
        # 找到所有台风的集合
        # if obj.hasattr('typhoonList'):
        # 注意判断字典中是否包含指定key,不能使用 hasattr 的方法进行panduan
        # if hasattr(obj, 'typhoonList'):
        if 'typhoonList' in obj.keys():
            list_typhoons = obj['typhoonList']
            for ty_temp in list_typhoons:
                # [2723975, 'nameless', '热带低压', None, '20210022', 20210022, None, 'stop']
                # 根据台风编号找到是否存在对应的台风，并获取台风 英文名(index=1)+中文名(index=2)+台风路径网的id(index=0)
                temp_code: str = ty_temp[4]
                if temp_code == ty_code:
                    temp_id: int = ty_temp[0]
                    temp_name_ch: str = ty_temp[2]
                    temp_name_en: str = ty_temp[1]
                    ty_obj = TyDetailMidModel(ty_code, temp_id, temp_name_en, temp_name_ch)
                    break
                pass
            pass

        return ty_obj

    def get_ty_path(self, ty_id: int):
        baseUrl: str = 'typhoon.nmc.cn'
        # http://typhoon.nmc.cn/weatherservice/typhoon/jsons/view_2726099
        # target_url = f'{url}_{ty_id}'
        headers = {
            'User-Agent': get_agent()}
        conn = http.client.HTTPConnection(baseUrl)
        conn.request('GET', f"/weatherservice/typhoon/jsons/view_{str(ty_id)}", headers=headers)
        res = conn.getresponse()
        content = res.read().decode('utf-8')
        index: int = len(f'typhoon_jsons_view_{str(ty_id)}') + 1
        new_json = content[index:-1]

        #    raise JSONDecodeError("Extra data", s, end)
        # json.decoder.JSONDecodeError: Extra data: line 1 column 10 (char 9)
        '''
            typhoon_jsons_view_2726099(
                {"typhoon":
                    [2726099,
                    "NYATOH",
                    "妮亚图",
                    2121,
                    2121,
                    null,
                    "名字来源：马来西亚；意为：一种在东南亚热带雨林环境中生长的树木。",
                    "stop",
                    [
                        [
                    0   2726232,    
                    1    "202111300000",
                    2    1638230400000,  //ts
                    3    "TS",   // ty_type 
                    4    139.2,  // 经度-lon
                    5    12.6,   // 纬度-lat
                    6    998,   // bp
                    7    18,
                    8    "WNW",
                    9    15,
                    10    [...],
                    11   {
                            "BABJ": [
                            [
                                12,
                                "202111300000",
                                137.6,
                                13.2,
                                990,
                                23,
                                "BABJ",
                                "TS"
                            ],...
                            ]
                         }
                        ],
                        ...
                    ]
        '''
        ty_path_obj = json.loads(new_json, strict=False)
        tyPathMidModel: TyPathMidModel = {}
        if 'typhoon' in ty_path_obj.keys():
            ty_group_detail = ty_path_obj['typhoon']

            ty_group_list: [] = ty_group_detail[8]
            ty_realdata_list: [] = []
            for temp_ty_group in ty_group_list:
                forecast_ty_path_list: [] = []
                # TODO:[*] 22-06-20 查询2106号台风时出现了错误
                # ERROR: 'NoneType' object has no attribute 'keys'
                # 此处需要加入判断 temp_ty_group[11] 是否包含 keys
                if hasattr(temp_ty_group[11], 'keys') and 'BABJ' in temp_ty_group[11].keys():
                    '''
                        "BABJ": [
                            [
                              0  12,                hours
                              1  "202111300000",    timestamp
                              2  137.6,             lon
                              3  13.2,              lat
                              4  990,               bp
                              5  23,
                              6  "BABJ",
                              7  "TS"               ty_type
                            ],...
                            ]
                    '''
                    temp_forecast_ty_path_list = temp_ty_group[11]['BABJ']
                    for temp_forecast_ty_path in temp_forecast_ty_path_list:
                        if len(temp_forecast_ty_path) >= 8:
                            # TODO:[-] 22-04-06 注意此处的 timestamp 实际是 utc 时间的 yyyymmddHHMM
                            hours: int = temp_forecast_ty_path[0]
                            forecast_dt_str_utc: str = temp_forecast_ty_path[1]
                            forecast_dt: datetime = arrow.get(forecast_dt_str_utc,
                                                              'YYYYMMDDHHmm').datetime + timedelta(
                                hours=hours)
                            forcast_ts = forecast_dt.timestamp()

                            forecast_ty_path_list.append(
                                TyForecastRealDataMidModel(temp_forecast_ty_path[3], temp_forecast_ty_path[2],
                                                           temp_forecast_ty_path[4], int(forcast_ts),
                                                           temp_forecast_ty_path[7], []))
                            pass

                        pass
                ty_realdata_list.append(
                    TyForecastRealDataMidModel(temp_ty_group[5], temp_ty_group[4], temp_ty_group[6], temp_ty_group[2],
                                               temp_ty_group[3], forecast_ty_path_list))
                pass
            tyPathMidModel = TyPathMidModel(ty_group_detail[0], ty_group_detail[3], ty_group_detail[1],
                                            ty_group_detail[2],
                                            ty_realdata_list)
        return tyPathMidModel
