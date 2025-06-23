from typing import Any

import rasterio
import numpy as np
from rasterio.features import shapes
import matplotlib.pyplot as plt
from shapely.geometry import mapping, LineString
import geojson


def extract_iso_surface(threshold_val: float, read_path: str, out_put_json_path: str, out_put_tif_path: str):
    """
        提取等值面
    :return:
    """
    # 定义阈值（请根据实际情况修改）
    MIN = 0
    nodata = None
    profile: Any = None
    # 打开 GeoTIFF 文件（假设文件名为 "input.tif"）
    with rasterio.open(read_path) as src:
        data = src.read(1)  # 读取第一个波段
        transform = src.transform  # 仿射变换参数
        profile = src.profile.copy()  # 复制原始影像信息，用于写入新 GeoTIFF
    # 创建布尔掩膜：选取像元值小于阈值的部分，同时排除 nodata 值（如果存在）
    mask_condition = data > threshold_val
    if nodata is not None:
        mask_condition = mask_condition & (data != nodata)

    # 将布尔掩膜转换为二值数组（0/1），方便后续提取连续区域
    binary = mask_condition.astype(np.uint8)

    # TODO:[-] 25-06-10 对于0值，填充为nan
    binary = binary.astype('float32')
    binary[binary == 0] = np.nan

    # --- 2. 利用 rasterio.features.shapes 提取区域，并存储为 GeoJSON ---
    # 为矢量化构建一个有效掩膜：非 NaN 的部分置为 1
    vector_mask = ~np.isnan(binary)
    vector_mask = vector_mask.astype(np.uint8)

    # 利用 rasterio.features.shapes 提取连续区域几何：遍历生成器，返回 (geometry, value) 对
    features = []
    for geom, value in shapes(binary, mask=(binary == 1), transform=transform):
        # 只保留数值为 1 的区域
        if value == 1:
            # 将提取的几何添加为 geojson.Feature，可以附加属性说明条件
            features.append(geojson.Feature(geometry=geom, properties={"threshold": threshold_val}))

    # 将所有 Feature 组合为 FeatureCollection，并写入 GeoJSON 文件
    feature_collection = geojson.FeatureCollection(features)
    with open(out_put_json_path, "w") as f:
        geojson.dump(feature_collection, f)

    print(f'提取的面已保存到 {out_put_json_path} 文件中.')

    # --- 3. 同时保存二值区域掩膜为新的 GeoTIFF ---
    # 更新保存参数：数据类型改为 float32，nodata 也设置为 NaN
    profile.update({
        "driver": "GTiff",
        "dtype": "float32",
        "count": 1,
        "nodata": np.nan,
    })

    with rasterio.open(out_put_tif_path, "w", **profile) as dst:
        dst.write(binary, 1)

    print(f"GeoTIFF 已保存至 {out_put_tif_path}")


def main():
    val: float = 150
    # read_path: str = r'E:\01data\99test\flood_geo\output_processed.tif'
    # out_put_json_path: str = r'E:\01data\99test\flood_geo\output_processed_gt200.geojson'
    # out_put_tif_path: str = r'E:\01data\99test\flood_geo\output_processed_gt200.tif'
    read_path: str = r'E:\01data\99test\flood_geo\ningbo_test_250611_masked.tif'
    out_put_json_path: str = r'E:\01data\99test\flood_geo\ningbo_test_250611_masked_gt150.geojson'
    out_put_tif_path: str = r'E:\01data\99test\flood_geo\ningbo_test_250611_masked_gt150.tif'
    extract_iso_surface(val, read_path, out_put_json_path, out_put_tif_path)
    pass


if __name__ == '__main__':
    main()
