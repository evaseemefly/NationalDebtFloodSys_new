from shapely import wkt
from shapely.geometry import mapping


def convert_coordinate_order(wkt_string):
    """
        修正 WKT 字符串中的坐标顺haixi序
        (lat,lng) => (lng,lat)
    @param wkt_string:
    @return:
    """
    # 解析 WKT 字符串
    shape_obj = wkt.loads(wkt_string)

    # 获取坐标并交换顺序
    corrected_shape = type(shape_obj)([(y, x) for x, y in shape_obj.exterior.coords])

    # 返回修正后的 GeoJSON
    return mapping(corrected_shape)
