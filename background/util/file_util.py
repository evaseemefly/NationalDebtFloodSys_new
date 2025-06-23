from pathlib import Path
from collections import defaultdict

from commons.enums import TYGroupTypeEnum


class FileExplorer:
    def __init__(self, directory_path):
        self.path = Path(directory_path)

    def get_all_files(self):
        """获取所有文件"""
        if not self.path.exists() or not self.path.is_dir():
            return []
        return [f for f in self.path.rglob("*") if f.is_file()]

    def get_files_by_type(self):
        """按文件类型分组"""
        files_by_type = defaultdict(list)

        for file in self.get_all_files():
            extension = file.suffix.lower() or "无扩展名"
            files_by_type[extension].append(file)

        return dict(files_by_type)

    def get_directory_size(self):
        """计算目录总大小"""
        total_size = 0
        for file in self.get_all_files():
            total_size += file.stat().st_size
        return total_size

    def find_files(self, pattern):
        """根据模式查找文件"""
        return list(self.path.rglob(pattern))


def get_grouppath_type(path_stamp: str) -> TYGroupTypeEnum:
    """
        根据路径 stamp 获取集合路径枚举
    :param path_stamp:
    :return:
    """
    switch_dict: dict = {
        'center': TYGroupTypeEnum.CENTER,
        'fast': TYGroupTypeEnum.FAST,
        'left': TYGroupTypeEnum.LEFT,
        'right': TYGroupTypeEnum.RIGHT,
        'slow': TYGroupTypeEnum.SLOW
    }
    return switch_dict.get(path_stamp, TYGroupTypeEnum.NONE)
