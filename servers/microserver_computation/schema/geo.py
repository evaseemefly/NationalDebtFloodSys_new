from typing import Dict, List, Union, Any, Optional

# Literal 是从 Python 3.8 开始加入标准库 typing 模块中的，因此在 Python 3.7 或更早版本中，你需要通过 typing_extensions 包来使用 Literal。
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel, Field, validator
from shapely.geometry import Polygon, mapping, shape
from geoalchemy2.elements import WKBElement
# 序列化 WKTElement使用
from geoalchemy2.shape import to_shape
from shapely.geometry import mapping, Polygon
import json


def tuple_to_list(obj: Any) -> Any:
    """
    递归地将所有元组转换为列表
    """
    if isinstance(obj, tuple):
        return [tuple_to_list(item) for item in obj]
    elif isinstance(obj, list):
        return [tuple_to_list(item) for item in obj]
    else:
        return obj


class GeoJSONPolygon(BaseModel):
    """GeoJSON 多边形几何对象"""
    type: Literal["Polygon"] = Field("Polygon", const=True)
    coordinates: List[List[List[float]]]

    @validator("coordinates")
    def validate_coordinates(cls, v):
        if not v or not isinstance(v, list) or not v[0] or len(v[0]) < 4:
            raise ValueError("多边形坐标必须至少包含4个点形成一个闭合环")
        # 检查第一个环是否闭合
        first_ring = v[0]
        if first_ring[0] != first_ring[-1]:
            raise ValueError("多边形的第一个环必须闭合（首尾点相同）")
        return v


class GeoFloodPolygonSchema(BaseModel):
    """
        淹没多边形 schema
    """
    value: Optional[float]
    ty_code: str
    name: str
    description: str
    properties: Dict[str, Any]  # 改为字典类型
    geom: GeoJSONPolygon
    issue_time: int
    flood_level: int

    @validator("properties", pre=True)
    def parse_properties(cls, v):
        """将 properties 字符串转换为字典"""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("properties 必须是有效的 JSON 字符串")
        return v

    @validator("geom", pre=True)
    def parse_geom(cls, v):
        """处理不同格式的几何数据"""
        from shapely.geometry import mapping, Polygon
        import json

        # 情况 1：已经是 GeoJSON 格式的字典
        if isinstance(v, dict):
            if v.get("type") == "Polygon":
                return v
            raise ValueError("几何数据必须是 Polygon 类型")

        # 情况 2：WKBElement 类型
        from geoalchemy2.elements import WKBElement, WKTElement
        if isinstance(v, WKBElement):
            try:
                from shapely import wkb
                # 将 WKBElement 转换为 Shapely 对象
                geom = wkb.loads(bytes(v.data))
                if not isinstance(geom, Polygon):
                    raise ValueError("几何数据必须是 Polygon 类型")
                return mapping(geom)
            except Exception as e:
                raise ValueError(f"无法解析 WKBElement: {str(e)}")

        # 情况 3：WKTElement 类型（原始数据中常见）
        if isinstance(v, WKTElement):
            try:
                from shapely import wkt
                # 方法一：有错误，弃用
                # 注意:此处不能直接将 WKTElement => str
                #  __str__ returned non-string (type dict) (type=value_error)
                # wkt_str: str = str(v)
                # geom = wkt.loads(wkt_str)
                # if not isinstance(geom, Polygon):
                #     raise ValueError("几何数据必须是 Polygon 类型")
                # return mapping(geom)
                # 方法2： WKTElement 为 dict，使用以下方式进行转换
                # 通过 to_shape 函数将 WKTElement 转换为 Shapely 几何对象
                # ERROR: 无法解析 WKTElement: Only str is accepted. (type=value_error)
                # {TypeError}TypeError('Only str is accepted.')
                # 注意： 此处的 v.data 为字典 ，eg:
                """
                    eg:
                    {'coordinates': (((122.081828, 29.969271), (122.082908, 29.969271), (122.082908, 29.968191), (122.081828, 29.968191), (122.081828, 29.969271)),), 'type': 'Polygon'}
                    只不过其中的坐标使用的是元组。这就导致了在使用 to_shape 或传统的解析方式时出错，因为这些方法都期望接收到字符串形式的 WKT。
                    解决方案是：在 parse_geom 的分支中检测到 v 为 WKTElement 后，判断其 .data 是否为字典。如果是，就对其中的坐标进行“元组到列表”的转换（递归转换确保所有嵌套元组都变为列表），然后直接返回这个字典即可。
                """
                # shapely_geom = to_shape(v)
                # # 检查是否为 Polygon
                # if not isinstance(shapely_geom, Polygon):
                #     raise ValueError("几何数据必须是 Polygon 类型")
                # # 使用 mapping 获取 GeoJSON 字典
                # return mapping(shapely_geom)
                if isinstance(v.data, dict):
                    # v.data 已包含 GeoJSON 数据，但坐标为元组形式
                    data = v.data
                    if data.get("type") != "Polygon":
                        raise ValueError("几何数据必须是 Polygon 类型")
                    # 递归地将所有元组转换为列表
                    data["coordinates"] = tuple_to_list(data["coordinates"])
                    return data
                else:
                    # 如果 v.data 不是字典，尝试将其转换为字符串后解析
                    wkt_str = v.data
                    if not isinstance(wkt_str, str):
                        wkt_str = wkt_str.decode("utf-8")
                    geom = wkt.loads(wkt_str)
                    if not isinstance(geom, Polygon):
                        raise ValueError("几何数据必须是 Polygon 类型")
                    return mapping(geom)

            except Exception as e:
                raise ValueError(f"无法解析 WKTElement: {str(e)}")

        # 情况 4：字符串形式，可能是 WKT 或 GeoJSON 字符串
        if isinstance(v, str):
            try:
                # 尝试作为 GeoJSON 解析
                geojson = json.loads(v)
                if geojson.get("type") == "Polygon":
                    return geojson
                elif "coordinates" in geojson and "type" in geojson:
                    return geojson
            except json.JSONDecodeError:
                # 不是 JSON，则尝试解析 WKT
                try:
                    from shapely import wkt
                    geom = wkt.loads(v)
                    if not isinstance(geom, Polygon):
                        raise ValueError("几何数据必须是 Polygon 类型")
                    return mapping(geom)
                except Exception:
                    pass
            raise ValueError("无法将字符串解析为有效的 GeoJSON 或 WKT 多边形")

        raise ValueError(f"不支持的几何数据类型: {type(v)}")

    class Config:
        orm_mode = True
        json_encoders = {
            # 自定义 JSON 编码器: 将 WKBElement 转换为 GeoJSON 形式（实际可按需调整）
            # 这里假设传入时已经经过 validator 处理，所以一般不会直接编码 WKBElement
        }


class GeoPolygonSchema(BaseModel):
    value: float
    ty_code: str
    name: str
    description: str
    properties: Dict[str, Any]  # 改为字典类型
    geom: GeoJSONPolygon
    issue_time: int

    @validator("properties", pre=True)
    def parse_properties(cls, v):
        """将 properties 字符串转换为字典"""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("properties 必须是有效的 JSON 字符串")
        return v

    @validator("geom", pre=True)
    def parse_geom(cls, v):
        """处理不同格式的几何数据"""
        from shapely.geometry import mapping, Polygon
        import json

        # 情况 1：已经是 GeoJSON 格式的字典
        if isinstance(v, dict):
            if v.get("type") == "Polygon":
                return v
            raise ValueError("几何数据必须是 Polygon 类型")

        # 情况 2：WKBElement 类型
        from geoalchemy2.elements import WKBElement, WKTElement
        if isinstance(v, WKBElement):
            try:
                from shapely import wkb
                # 将 WKBElement 转换为 Shapely 对象
                geom = wkb.loads(bytes(v.data))
                if not isinstance(geom, Polygon):
                    raise ValueError("几何数据必须是 Polygon 类型")
                return mapping(geom)
            except Exception as e:
                raise ValueError(f"无法解析 WKBElement: {str(e)}")

        # 情况 3：WKTElement 类型（原始数据中常见）
        if isinstance(v, WKTElement):
            try:
                from shapely import wkt
                # 方法一：有错误，弃用
                # 注意:此处不能直接将 WKTElement => str
                #  __str__ returned non-string (type dict) (type=value_error)
                # wkt_str: str = str(v)
                # geom = wkt.loads(wkt_str)
                # if not isinstance(geom, Polygon):
                #     raise ValueError("几何数据必须是 Polygon 类型")
                # return mapping(geom)
                # 方法2： WKTElement 为 dict，使用以下方式进行转换
                # 通过 to_shape 函数将 WKTElement 转换为 Shapely 几何对象
                # ERROR: 无法解析 WKTElement: Only str is accepted. (type=value_error)
                # {TypeError}TypeError('Only str is accepted.')
                # 注意： 此处的 v.data 为字典 ，eg:
                """
                    eg:
                    {'coordinates': (((122.081828, 29.969271), (122.082908, 29.969271), (122.082908, 29.968191), (122.081828, 29.968191), (122.081828, 29.969271)),), 'type': 'Polygon'}
                    只不过其中的坐标使用的是元组。这就导致了在使用 to_shape 或传统的解析方式时出错，因为这些方法都期望接收到字符串形式的 WKT。
                    解决方案是：在 parse_geom 的分支中检测到 v 为 WKTElement 后，判断其 .data 是否为字典。如果是，就对其中的坐标进行“元组到列表”的转换（递归转换确保所有嵌套元组都变为列表），然后直接返回这个字典即可。
                """
                # shapely_geom = to_shape(v)
                # # 检查是否为 Polygon
                # if not isinstance(shapely_geom, Polygon):
                #     raise ValueError("几何数据必须是 Polygon 类型")
                # # 使用 mapping 获取 GeoJSON 字典
                # return mapping(shapely_geom)
                if isinstance(v.data, dict):
                    # v.data 已包含 GeoJSON 数据，但坐标为元组形式
                    data = v.data
                    if data.get("type") != "Polygon":
                        raise ValueError("几何数据必须是 Polygon 类型")
                    # 递归地将所有元组转换为列表
                    data["coordinates"] = tuple_to_list(data["coordinates"])
                    return data
                else:
                    # 如果 v.data 不是字典，尝试将其转换为字符串后解析
                    wkt_str = v.data
                    if not isinstance(wkt_str, str):
                        wkt_str = wkt_str.decode("utf-8")
                    geom = wkt.loads(wkt_str)
                    if not isinstance(geom, Polygon):
                        raise ValueError("几何数据必须是 Polygon 类型")
                    return mapping(geom)

            except Exception as e:
                raise ValueError(f"无法解析 WKTElement: {str(e)}")

        # 情况 4：字符串形式，可能是 WKT 或 GeoJSON 字符串
        if isinstance(v, str):
            try:
                # 尝试作为 GeoJSON 解析
                geojson = json.loads(v)
                if geojson.get("type") == "Polygon":
                    return geojson
                elif "coordinates" in geojson and "type" in geojson:
                    return geojson
            except json.JSONDecodeError:
                # 不是 JSON，则尝试解析 WKT
                try:
                    from shapely import wkt
                    geom = wkt.loads(v)
                    if not isinstance(geom, Polygon):
                        raise ValueError("几何数据必须是 Polygon 类型")
                    return mapping(geom)
                except Exception:
                    pass
            raise ValueError("无法将字符串解析为有效的 GeoJSON 或 WKT 多边形")

        raise ValueError(f"不支持的几何数据类型: {type(v)}")

    class Config:
        orm_mode = True
        json_encoders = {
            # 自定义 JSON 编码器: 将 WKBElement 转换为 GeoJSON 形式（实际可按需调整）
            # 这里假设传入时已经经过 validator 处理，所以一般不会直接编码 WKBElement
        }


class FloodPolygonFeatureCollectionSchema(BaseModel):
    """
        + 25-06-16 新加入的 与前台 types/geo.ts => FloodPolygonFeatureCollection
    """
    type: str
    features: List[GeoFloodPolygonSchema]
