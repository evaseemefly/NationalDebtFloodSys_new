import json
import pathlib
from datetime import datetime, timezone, timedelta
from typing import List

from schemas import TyphoonPathComplexDetailSchema, TyphoonPointSchema


class TyphoonPathExecutor:
    """
        台风路径执行者
    """

    def __init__(self, ty_path_schema: TyphoonPathComplexDetailSchema, out_put_path: str):
        self.ty_path_schema = ty_path_schema
        self.out_put_path = out_put_path

    def ty_detail_execute(self):
        """
            生成台风基础信息 json
        :param ty_path_schema:
        :param out_put:
        :return:
        """

        ty_detail = self.ty_path_schema.tyDetail
        ty_detail_json_content = {
            "tc_num": str(ty_detail.tyCode),
            "tc_name_en": ty_detail.tyNameEn,
            "tc_name_cn": ty_detail.tyNameCh
        }
        # --- 1. 生成 a.json 文件 ---
        print(f'正在生成 处理:{ty_detail.tyCode}台风路径->json')
        # 写入文件
        # ensure_ascii=False 确保中文字符正确显示
        # indent=4 使 JSON 文件格式优美，易于阅读
        file_name: str = 'tc_info.json'
        out_put_path: str = str(pathlib.Path(self.out_put_path) / file_name)
        with open(out_put_path, 'w', encoding='utf-8') as f:
            json.dump(ty_detail_json_content, f, ensure_ascii=False, indent=4)
        print(f"处理:{ty_detail.tyCode}台风detail->json 完毕{out_put_path}")

    def ty_path_list_execute(self):
        ty_detail = self.ty_path_schema.tyDetail
        ty_path: List[TyphoonPointSchema] = self.ty_path_schema.tyPathList
        # 定义北京时间时区 (UTC+8)
        cst_tz = timezone(timedelta(hours=8))

        file_name: str = 'tc_track_info.txt'
        out_put_path: str = str(pathlib.Path(self.out_put_path) / file_name)
        # --- 2. 生成 b.txt 文件 ---
        print(f"处理:{ty_detail.tyCode}台风路径->json ing")
        # 使用 with open 确保文件操作安全
        with open(out_put_path, 'w', encoding='utf-8') as f:
            # 写入文件头
            f.write("dateCST lonTC latTC presTC\n")

            # 遍历路径列表
            for path_point in ty_path:
                utc_dt = path_point.forecastDt
                # 直接将其转换为北京时间
                cst_dt = utc_dt.astimezone(cst_tz)
                # 格式化时间为 YYYYMMDDHH 字符串
                date_cst_str = cst_dt.strftime('%Y%m%d%H')
                # 提取经纬度和气压
                lon = path_point.lon
                lat = path_point.lat
                bp = path_point.bp
                # 格式化为目标字符串并写入文件
                # 经纬度保留一位小数，气压取整
                line = f"{date_cst_str} {lon:.1f} {lat:.1f} {int(bp)}\n"
                f.write(line)

        print(f"处理:{ty_detail.tyCode}台风路径->json 完毕{out_put_path}")
