import pathlib

from shapely.geometry import Polygon, mapping, shape
import geojson

READPATH: str = r'E:\01data\99test\flood_geo\ningbo_test_250611_masked_gt200.geojson'


def chaikin_smoothing(coords, refinements=3):
    for _ in range(refinements):
        new_coords = []
        for i in range(len(coords) - 1):
            p1 = coords[i]
            p2 = coords[i + 1]
            Q = (0.75 * p1[0] + 0.25 * p2[0], 0.75 * p1[1] + 0.25 * p2[1])
            R = (0.25 * p1[0] + 0.75 * p2[0], 0.25 * p1[1] + 0.75 * p2[1])
            new_coords.extend([Q, R])
        # 闭合多边形
        new_coords.append(new_coords[0])
        coords = new_coords
    return coords


def smoothed_polygon(json_path: str, out_put_path: str):
    # 加载 geojson 文件
    with open(json_path, "r") as f:
        data = geojson.load(f)

    # 遍历每个多边形并平滑
    for feature in data['features']:
        geom = shape(feature['geometry'])
        if geom.geom_type == 'Polygon':
            outer_ring = list(geom.exterior.coords)
            smoothed_ring = chaikin_smoothing(outer_ring, refinements=3)
            smoothed_poly = Polygon(smoothed_ring)
            feature['geometry'] = mapping(smoothed_poly)

    # 保存结果
    file_name: str = f'smoothed_output.geojson'
    out_put_full_path: str = str(pathlib.Path(out_put_path) / file_name)
    with open(out_put_full_path, "w") as f:
        geojson.dump(data, f)


def main():
    out_put_path: str = r'E:\01data\99test\flood_geo_smooted'
    # 平滑多边形
    smoothed_polygon(READPATH, out_put_path)


if __name__ == "__main__":
    main()
