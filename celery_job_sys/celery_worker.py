import pathlib
import subprocess
import os
from datetime import datetime

from config.base_config import StoreConfig
from config.celery_config import celery_app
from config.config import base_setting
from schemas import TyphoonPathComplexDetailSchema
from tasks.ty_path_executor import ty_detail_executor, ty_path_list_executor, TyphoonPathExecutor


@celery_app.task(name="ty_group")
def execute_shell_job(params_dict: dict):
    """
    Celery 任务：执行 shell 脚本并更新数据库

    params {'tyDetail': {'timeStamp': 1752461190841, 'tyCode': 2504, 'tyNameCh': '丹娜丝', 'tyNameEn': 'DANAS'}, 'tyPathList': [{'forecastDt': '2025-07-06T15:00:00Z', 'lat': 23.3, 'lon': 120.0, 'bp': 950.0, 'isForecast': False, 'tyType': 'STY'}, {'forecastDt': '2025-07-06T16:00:00Z', 'lat': 23.4, 'lon': 120.2, 'bp': 960.0, 'isForecast': False, 'tyType': 'TY'}, {'forecastDt': '2025-07-06T17:00:00Z', 'lat': 23.5, 'lon': 120.4, 'bp': 960.0, 'isForecast': False, 'tyType': 'TY'}, {'forecastDt': '2025-07-06T18:00:00Z', 'lat': 23.7, 'lon': 120.7, 'bp': 965.0, 'isForecast': False, 'tyType': 'TY'}]}
    """
    try:
        user_name: str = 'admin'
        ty_path_complex_detail: TyphoonPathComplexDetailSchema = TyphoonPathComplexDetailSchema.model_validate(
            params_dict)
        ty_path_path_str: str = StoreConfig.TY_SOURCE_PATH
        ty_path_path: pathlib.Path = pathlib.Path(ty_path_path_str)

        """台风路径存储路径"""
        # step1: 将 台风路径信息存储为 json
        store_path: pathlib.Path = ty_path_path / 'user_in' / user_name
        if store_path.exists() is False:
            store_path.mkdir(parents=True, exist_ok=True)
        # 生成对应的台风路径文件
        ty_path_list_executor = TyphoonPathExecutor(ty_path_complex_detail, str(store_path))
        ty_path_list_executor.ty_detail_execute()
        ty_path_list_executor.ty_path_list_execute()
        pass
        # ----------------

        script_path = os.path.join(base_setting.SCRIPTS_DIR, "run_surge.sh")
        if not os.path.exists(script_path):
            raise FileNotFoundError("Shell script not found")

        # step1: 执行脚本之前需要提供台风路径信息
        # TODO:[*] 25-07-11 处理完成此步骤
        # 提供台风路径信息——由前端提交后 fastapi - celery => task

        # step2: 运行脚本，生成五条集合台风路径和五种路径对应的增水【场和站点增水】
        # run_surge.sh
        # 将参数传递给 shell 脚本
        command = ['bash', script_path]

        # step3: 等待结束后处理——有WKDIR/user_out/admin/log.flag_surge日志文件生成，说明运行结束
        # 3-1: 集合路径
        # 3-2: 站点集合增水预报结果
        # 3-3: 增水场(处理流程较多)

        # 在 output 目录执行脚本
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            cwd=base_setting.OUTPUT_DIR
        )

        # 假设脚本成功后会生成一个以 task_id 命名的文件
        # output_file_path = os.path.join(settings.OUTPUT_DIR, f"{job.task_id}.log")

        # job.status = "SUCCESS"
        # job.output_file = output_file_path
        # return {"status": "SUCCESS", "output_file": output_file_path}

    except Exception as e:
        # job.status = "FAILURE"
        # 记录错误信息，实际项目中可以用更详细的日志
        print(f"Task failed: {str(e)}")
        raise e  # 重新抛出异常，Celery 会标记任务为 FAILURE
    finally:
        # job.finished_at = datetime.utcnow()
        # db.commit()
        # db.close()
        pass
