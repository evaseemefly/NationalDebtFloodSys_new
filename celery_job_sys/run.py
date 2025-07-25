import asyncio

from celery_worker import post_process


def main():
    # 运行主协程，启动整个异步程序
    asyncio.run(post_process())
    pass


if __name__ == '__main__':
    main()
