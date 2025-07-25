import pathlib
import subprocess
import os
from datetime import datetime

import arrow
import time
import asyncio

from config.base_config import StoreConfig
from config.celery_config import celery_app
from config.config import base_setting
from schemas import TyphoonPathComplexDetailSchema
from tasks.station_surge_executor import StationSurgeExecutor
from tasks.surge_raster_executor import SurgeRasterExecutor
from tasks.ty_path_executor import TyphoonPathExecutor, TyphoonGroupPathExecutor


@celery_app.task(name="ty_group")
def execute_shell_job(params_dict: dict):
    """
    Celery 任务：执行 shell 脚本并更新数据库

    params {'tyDetail': {'timeStamp': 1752461190841, 'tyCode': 2504, 'tyNameCh': '丹娜丝', 'tyNameEn': 'DANAS'}, 'tyPathList': [{'forecastDt': '2025-07-06T15:00:00Z', 'lat': 23.3, 'lon': 120.0, 'bp': 950.0, 'isForecast': False, 'tyType': 'STY'}, {'forecastDt': '2025-07-06T16:00:00Z', 'lat': 23.4, 'lon': 120.2, 'bp': 960.0, 'isForecast': False, 'tyType': 'TY'}, {'forecastDt': '2025-07-06T17:00:00Z', 'lat': 23.5, 'lon': 120.4, 'bp': 960.0, 'isForecast': False, 'tyType': 'TY'}, {'forecastDt': '2025-07-06T18:00:00Z', 'lat': 23.7, 'lon': 120.7, 'bp': 965.0, 'isForecast': False, 'tyType': 'TY'}]}
    """
    try:
        user_name: str = 'admin'
        user_id: int = 1
        ty_path_complex_detail: TyphoonPathComplexDetailSchema = TyphoonPathComplexDetailSchema.model_validate(
            params_dict)
        ty_path_path_str: str = StoreConfig.TY_SOURCE_PATH
        ty_path_path: pathlib.Path = pathlib.Path(ty_path_path_str)
        TY_CODE: str = str(ty_path_complex_detail.tyDetail.tyCode)
        ty_name_ch: str = ty_path_complex_detail.tyDetail.tyNameCh
        ty_name_en: str = ty_path_complex_detail.tyDetail.tyNameEn
        ISSUE_TS: int = arrow.utcnow().int_timestamp
        """提交作业时间"""
        FORECAST_TS: int = arrow.get(ty_path_complex_detail.tyPathList[0].forecastDt).int_timestamp
        """预报起始时间"""

        """台风路径存储路径"""
        # step1: 将 台风路径信息存储为 json
        store_path: pathlib.Path = ty_path_path / 'user_in' / user_name
        # /Volumes/DRCC_DATA/02WORKSPACE/nation_flood/model_execute/surgeflood_wkdir/user_out/admin
        result_path: pathlib.Path = ty_path_path / 'surgeflood_wkdir' / 'user_out' / user_name
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

        # TODO:[-] 25-07-23 模型计算+后处理流程
        # 提供台风路径信息——由前端提交后 fastapi - celery => task
        was_successful = post_process()
        """后处理是否后处理是否完成"""

        # step2: 判断模型是否计算结束,若存在则执行后续流程
        if was_successful:
            # step3: 批量存储站点增水集合
            # TODO:[*] 25-07-17 未测试
            station_surge_executor = StationSurgeExecutor(read_path=str(result_path), ty_code=TY_CODE,
                                                          issue_ts=ISSUE_TS,
                                                          forecast_start_ts=FORECAST_TS)
            station_surge_executor.execute()

            # step4: 批量存储台风集合路径
            ty_grouppath_executor = TyphoonGroupPathExecutor(user_id, TY_CODE, ty_name_ch, ty_name_en, ISSUE_TS)
            ty_grouppath_executor.execute()

            # step5: 批量写入增水范围
            """
                增水栅格执行器
                center | fast | slow | left | right
            """
            surge_raster_executor = SurgeRasterExecutor(user_id, user_name, TY_CODE, ty_name_en, ty_name_ch, ISSUE_TS)
            surge_raster_executor.execute()

            # step6: 批量写入淹没范围

            # step4: 批量
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


async def async_check_model_completed(interval_seconds: int, timeout_mins: float):
    """
         一个异步的、周期性执行的任务。
         应加入退出条件，根据时间加入退出条件
    :param interval_seconds:
    :param timeout_mins:
    :return:
    """
    user_name: str = 'admin'
    ty_path_path_str: str = StoreConfig.TY_SOURCE_PATH
    ty_path_path: pathlib.Path = pathlib.Path(ty_path_path_str)
    ending_stamp_file_path: pathlib.Path = ty_path_path / 'surgeflood_wkdir' / 'user_out' / user_name / 'log.flag_surge'

    # --- 1. 初始化和记录开始时间 ---
    start_time = time.monotonic()  # 使用单调时钟记录开始时间，最适合测量时长
    timeout_seconds = timeout_mins * 60
    while True:
        # --- 2. 在每次循环开始时，检查是否超时 ---
        elapsed_time = time.monotonic() - start_time
        if elapsed_time > timeout_seconds:
            print(f"⌛️ [超时退出] 任务已运行超过 {timeout_mins} 分钟，自动结束。")
            return False
        current_str: str = arrow.now().format('YYYY-MM-DD HH:mm:ss')
        print(f"⏰ [Asyncio Task] 正在执行... 时间: {current_str}")
        if ending_stamp_file_path.exists():
            return True
        # 在异步任务中，必须使用 asyncio.sleep() 而不是 time.sleep()
        # asyncio.sleep() 不会阻塞整个事件循环
        current_str: str = arrow.now().format('YYYY-MM-DD HH:mm:ss')
        print(f"⏰ [等待中] 正在等待模型计算完成... 时间: {current_str}")
        await asyncio.sleep(interval_seconds)


async def main_workflow():
    """
    模拟您的异步主工作流程。
    """
    print("🚀 [Asyncio] 主工作流程开始...")

    for step in range(1, 10000):
        print(f"   - 主流程异步步骤 {step}/forever 正在执行...")
        # 模拟异步I/O操作，例如一次API调用
        await asyncio.sleep(1)

    print("✅ [Asyncio] 主工作流程结束。")


async def post_process():
    """
        启动异步重复任务并执行后续的任务
        model computed => product to store
    :return:
    """
    print("准备启动后台任务，等待模型计算完成...")

    TASK_INTERVAL = 3
    TIMEOUT_MINS = 2  # 设置为30分钟

    # 执行等待任务，并获取它的结果 (True 或 False)
    was_successful = await async_check_model_completed(TASK_INTERVAL, TIMEOUT_MINS)

    # 根据等待任务的结果，决定是否执行主流程
    if was_successful:
        print("✅ 模型计算已完成，现在开始执行主工作流程。")
        await main_workflow()
    else:
        print("❌ 模型计算超时，主工作流程将不会执行。")

    print("🎉 所有任务均已完成。")
    return was_successful
