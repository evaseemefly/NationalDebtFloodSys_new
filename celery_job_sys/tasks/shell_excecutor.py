import subprocess
import os
from datetime import datetime
from .celery_app import celery
from .database import SessionLocal
from .models import JobResult
from config import SCRIPTS_DIR, OUTPUT_DIR


@celery.task(bind=True)
def execute_shell_task(self, script_name, *args):
    """
    执行 shell 脚本的 Celery 任务。
    注意：数据库会话管理需要手动完成。
    """
    db = SessionLocal()  # 为每个任务创建一个新的数据库会话
    try:
        job = db.query(JobResult).filter(JobResult.task_id == self.request.id).first()
        if not job:
            # 理论上，提交任务时就应创建记录，这里作为保险
            return {'status': 'FAILURE', 'error': 'Job record not found in DB'}

        script_path = os.path.join(SCRIPTS_DIR, script_name)
        if not os.path.exists(script_path):
            job.status = 'FAILURE'
            raise FileNotFoundError(f"脚本文件未找到: {script_path}")

        # 确保输出目录存在
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # 约定：shell 脚本的第一个参数是 Celery 的 task_id
        command = ['bash', script_path, self.request.id] + list(args)

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,  # 失败时抛出异常
            cwd=OUTPUT_DIR  # 将工作目录设为 output，方便脚本创建文件
        )

        # 假设脚本生成的文件名与 task_id 一致
        generated_file_path = os.path.join(OUTPUT_DIR, f"{self.request.id}.txt")

        job.status = 'SUCCESS'
        if os.path.exists(generated_file_path):
            job.output_file = generated_file_path

        return {'status': 'SUCCESS', 'output': result.stdout, 'file': job.output_file}

    except Exception as e:
        job.status = 'FAILURE'
        # 以字符串形式记录错误，以便序列化
        # 使用 atexit 或 on_failure 处理器来处理更复杂的清理逻辑
        self.update_state(state='FAILURE', meta={'exc_type': type(e).__name__, 'exc_message': str(e)})
        raise e  # 重新抛出异常，Celery 会捕获并标记任务失败
    finally:
        job.finished_at = datetime.utcnow()
        db.commit()  # 提交所有更改
        db.close()  # 必须关闭会话，释放连接
