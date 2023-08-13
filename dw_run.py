import logging
import random
import time
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any

import sqlalchemy.exc

from kiwi import app, db

logger = logging.getLogger(__name__)


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument('--job', required=True,
                        help='脚本名称,如 adb_report_task_daily, once.fix_task_data')
    parser.add_argument('--mode', required=True, choices=['daily', 'rt', 'daemon'],
                        help='daily 模式:日期为昨天,临时表后缀为 daily;'
                             'rt 模式:日期为今天,临时表后缀为 rt;'
                             'daemon 模式:日期为今天,临时表后缀为 daemon;')
    parser.add_argument('--date', help='指定日期,格式为 2021-05-17。'
                                       '不同模式有默认值,可用该参数覆盖。')
    parser.add_argument('--suffix', help='指定临时表后缀。不同模式有默认值,可用该参数覆盖。')
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose logging')
    parser.add_argument('params', nargs='*', metavar='params')
    args = parser.parse_args()

    default_date = get_today()
    default_suffix = args.mode

    if args.mode == 'daily':
        default_date = get_yesterday()

    if args.date is None:
        args.date = default_date

    if args.suffix is None:
        args.suffix = default_suffix

    if args.verbose:
        app.debug = True

    app.start()
    run_job(args)


def run_job(args: Namespace, times=0):
    """运行单次脚本。如果发生数据库错误则重试。"""
    try:
        mod: Any = import_module('kiwi.{}'.format(args.job))
        job_date = convert_date(args.date)

        job = mod.Job(date=job_date, suffix=args.suffix)
        if args.params:
            job.run(*args.params)
        else:
            job.run()

        # 测试环境提示脚本已结束
        if app.config.get('fp'):
            logger.info('Job succeeded.')

    except sqlalchemy.exc.OperationalError as e:
        # 重试 5 次;测试环境不重试
        if times >= 5 or app.config.get('fp'):
            raise Exception('Hit retry limit') from e

        # 随机等待 10-15 秒
        delay_secs = 10 + round(5 * random.random(), 1)
        logger.warning('Caught exception, retry in %d seconds...', delay_secs, exc_info=True)
        db.remove_sessions()
        time.sleep(delay_secs)
        times += 1
        run_job(args, times)


def get_today() -> str:
    now = datetime.now()
    return now.strftime('%Y-%m-%d')


def get_yesterday() -> str:
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def convert_date(s: str) -> str:
    date = datetime.strptime(s, '%Y-%m-%d')
    return date.strftime('%Y%m%d')


if __name__ == '__main__':
    main()
