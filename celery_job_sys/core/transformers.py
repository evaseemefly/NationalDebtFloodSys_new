import pathlib
import os
from typing import Optional

import pandas as pd
import numpy as np
import pathlib
import xarray as xr
import rioxarray

from common.default import NONE_ID
from common.enums import ElementTypeEnum, RasterFileType, TyphoonGroupEnum
from common.exceptions import FileDontExists, FileReadError, FileTransformError
from common.util import get_ty_group_enum
from models.mid_models import IForecastProductFile, ForecastSurgeRasterFile


class SurgeTransformer:
    """
        + 基于全球风暴潮系统修改的预报产品转换器
        提取nc文件中的 zmax -> geotiff
    """

    def __init__(self, file: IForecastProductFile, task_id: int = NONE_ID):
        self.task_id = task_id
        self.file = file
        self._ds: Optional[xr.Dataset] = None
        """标准化后的 dataset """

    def read_data(self, var_name: str = "zmax"):
        """
            根据 self.file 读取文件并将ds 写入 self._ds
            若异常则不写入 ds
        :param var_name: variables 名称
        :return:
        """
        if self.file.exists():
            try:
                # step1-1: 读取指定文件
                data: xr.Dataset = xr.open_dataset(self.file.local_full_path)
                # step1-2: 读取陆地掩码

                # TODO:[-] 此处加入了根据阈值进行过滤的filter mask步骤
                data_mask = (data[var_name] <= -0.3) | (data[var_name] >= 0.3)
                # TODO:[-] 此部分不需要删除被掩码掉的部分，只填充nan
                filtered_ds_h = data.where(data_mask, drop=False)
                # step1-3: 对于维度进行倒叙排列
                data_standard: xr.Dataset = filtered_ds_h.sortby('lat', ascending=False)
                # step1-4: 设置空间坐标系与分辨率
                first_ds = data_standard[var_name]
                # 设置空间坐标系与分辨率
                first_ds.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
                first_ds.rio.write_crs("EPSG:4326", inplace=True)
                self._ds = first_ds
            except Exception as e:
                raise FileReadError
            pass
        else:
            raise FileDontExists()

    def out_put(self, compress="deflate", diver: str = 'GTiff') -> Optional[
        ForecastSurgeRasterFile]:
        """
            输出并转换为 geotiff
        :param compress: 压缩方法
        :param diver:   输出文件类型
        :return:
        """
        raster_file: Optional[ForecastSurgeRasterFile] = None
        """输出的栅格文件"""
        element_type: ElementTypeEnum = ElementTypeEnum.SURGE
        raster_type: RasterFileType = RasterFileType.GEOTIFF
        if self._ds is not None:
            try:
                file_splits = self.file.file_name.split('.')[:2]
                temp_group_type_str: str = self.file.file_name.split('.')[0].split('_')[1]
                temp_group_type: TyphoonGroupEnum = get_ty_group_enum(temp_group_type_str)
                """当前文件的 集合路径类型枚举"""
                file_splits.append('tif')
                transformer_file_name: str = '.'.join(file_splits)
                out_put_file_path: str = str(pathlib.Path(
                    self.file.local_root_path) / self.file.relative_path / transformer_file_name)
                self._ds.rio.to_raster(out_put_file_path, diver=diver, compress=compress)
                raster_file = ForecastSurgeRasterFile(raster_type, self.file.issue_ts, transformer_file_name,
                                                      self.file.relative_path,
                                                      self.file.local_root_path, temp_group_type)
            except Exception as e:
                raise FileTransformError()
        return raster_file
