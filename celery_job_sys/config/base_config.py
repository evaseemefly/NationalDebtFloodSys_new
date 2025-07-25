class StoreConfig:
    MODEL_PATH: str = r''
    """模型所在路径"""
    TY_SOURCE_PATH: str = r'/data_local'
    """模型根目录"""
    STORE_ROOT_PATH: str = r'/data_remote'
    """模型生成后的存储的根目录"""
    STORE_REMOTE_RELATIVE_PATH: str = r'/surgeflood_wkdir/user_out'
    """
        存储的REMOTE的相对路径
        eg: 对应本地目录: /Volumes/DRCC_DATA/02WORKSPACE/nation_flood/model_execute    
        /Volumes/DRCC_DATA/02WORKSPACE/nation_flood/model_execute  : TY_SOURCE_PATH
        surgeflood_wkdir/user_out                                  : STORE_REMOTE_RELATIVE_PATH
        完整路径:  /Volumes/DRCC_DATA/02WORKSPACE/nation_flood/model_execute/surgeflood_wkdir/user_out/admin
    """
