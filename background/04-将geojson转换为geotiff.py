import geopandas as gpd
import rasterio
from rasterio import features
import numpy as np


def convert_2_tiff(read_path: str, out_put_path: str):
    grid_unit: float = 0.00108
    # 1. 读取 GeoJSON 数据
    # geojson 内部每个 feature 的属性中拥有 "最大淹没深度(cm)" 字段
    gdf = gpd.read_file(read_path)

    # 检查数据投影（如果数据为经纬度，单位通常为度）
    # 如需提高精度或后续分析，可以考虑投影到合适的坐标系
    print("数据投影:", gdf.crs)

    # 2. 计算整个数据的范围，并设定输出分辨率（这里以 0.0001 度为例）
    xmin, ymin, xmax, ymax = gdf.total_bounds

    # 根据指定分辨率计算行列数
    resolution = grid_unit  # 单位与数据投影相同（如数据为经纬度则单位为度）
    width = int((xmax - xmin) / resolution)
    height = int((ymax - ymin) / resolution)

    # 构造仿射变换参数，从左上角开始
    transform = rasterio.transform.from_origin(xmin, ymax, resolution, resolution)

    # 3. 利用 features.rasterize 将矢量数据转换为二维数组
    # 填充值可设为 -9999（nodata）
    # 3. 栅格化时，使用 np.nan 作为填充值，
    #    并改为 float32 类型，否则 np.nan（浮点）不能存入 int 类型数组
    fill_value = np.nan

    # 利用每个网格的几何和属性值（假设字段为 "最大淹没深度(cm)"）
    # shapes = ((geom, value) for geom, value in zip(gdf.geometry, gdf["最大淹没深度(cm)"]))
    shapes = ((geom, value) for geom, value in zip(gdf.geometry, gdf["最大淹没深度(cm)"])
              if value is not None)

    burned = features.rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=fill_value,
        dtype='float32'  # 根据实际数据，也可使用 float32
    )

    # 4. 写数据为 GeoTIFF 文件
    with rasterio.open(
            out_put_path,
            "w",
            driver="GTiff",
            height=burned.shape[0],
            width=burned.shape[1],
            count=1,
            dtype='float32',
            crs=gdf.crs,  # 以原数据投影为准
            transform=transform,
    ) as dst:
        dst.write(burned, 1)

    print(f"GeoTIFF 文件 {out_put_path} 已生成。")


def main():
    read_path: str = r'E:\01data\99test\flood_geo\ningbo_test_250611_masked.json'
    out_put: str = r'E:\01data\99test\flood_geo\ningbo_test_250611_masked.tif'
    convert_2_tiff(read_path, out_put)
    pass


if __name__ == '__main__':
    main()
