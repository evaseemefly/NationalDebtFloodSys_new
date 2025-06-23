import geopandas as gpd
from shapely.ops import unary_union


def mask_polygon(forecast_file: str, mask_file: str):
    # 读取 geo_a.json 和 pols.shp 文件
    gdf_a = gpd.read_file(forecast_file)
    gdf_mask = gpd.read_file(mask_file)

    # 将掩码中的所有多边形合并为一个整体
    mask_union = gdf_mask.geometry.unary_union

    # 对 geo_a 中每个几何对象使用差集操作，剔除掉在掩码内部的部分
    gdf_a['geometry'] = gdf_a['geometry'].apply(lambda geom: geom.difference(mask_union))

    # 如果有空的几何（全被去除的情况），可以选择剔除
    gdf_a = gdf_a[~gdf_a['geometry'].is_empty]

    # 将结果保存为新的 GeoJSON 文件
    gdf_a.to_file('geo_a_processed.json', driver='GeoJSON')
    print("处理完成，结果保存为 geo_a_processed.json")


def main():
    # flood_forecast_file: str = r'C:\Users\evase\Documents\qgis\data\宁波_国债\geojson_file_center_sparse.json'
    flood_forecast_file: str = r'E:\01data\99test\flood_geo\surgeflood.max.2019080905_2019081020.json'
    mask_file: str = r'E:\02data\02-qigs_data\01-ningbo\宁波_陆地及海洋外边界_不含岛屿_面.shp'
    mask_polygon(flood_forecast_file, mask_file)
    pass


if __name__ == '__main__':
    main()
