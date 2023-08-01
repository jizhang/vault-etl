import inspect
import logging
import os.path
import re
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Iterator, List, Match, Optional, Set, TypeVar

from kiwi import db
from kiwi.consts import PRINT_SQL_SUFFIX

T = TypeVar('T')
Params = Optional[Dict[str, Any]]

logger = logging.getLogger(__name__)


def run(job, filename: str, drop_tmp: bool = True, params: Params = None):
    """
    在 dw 库中运行脚本,按分号切分,逐条运行。

    :param job: 运行参数,一般传 self,目前会用到 date 和 suffix 两项。
    :param filename: SQL 文件名,使用模块相对路径。
    :param drop_tmp: 是否自动删除临时表,默认为 True。
    :param params: 额外的参数替换
    :return:
    """
    # read file
    sql_path = os.path.dirname(os.path.abspath(inspect.stack()[1][1]))
    sql_file = os.path.join(sql_path, filename)
    with open(sql_file) as f:
        sql_content = f.read()

    # remove comment
    sql_content = re.sub(r'/(?s)\*.*?\*/', '', sql_content)
    sql_content = re.sub(r'(?m)--\s+.*$', '', sql_content)

    # replace params
    sql_content = replace_params(job, sql_content, params)

    # split
    sqls = [i.strip() for i in sql_content.split(';') if i.strip()]

    # run
    engine = db.engine('dw')  # TODO
    tmp_tables: Set[str] = set()
    for sql in sqls:
        run_sql(engine, sql)
        collect_tmp_tables(tmp_tables, sql)

    if drop_tmp:
        drop_tmp_tables(engine, tmp_tables)


def run_sql(engine, sql: str):
    """执行 SQL 语句"""
    sql = sql.replace('%', '%%')
    sql_result = engine.execute(sql + PRINT_SQL_SUFFIX)
    logger.info('affected rows {}'.format(sql_result.rowcount))


def replace_params(job, sql_content: str, params: Params = None):
    """参数替换"""
    sql_content = sql_content.replace('{suffix}', job.suffix)
    sql_content = sql_content.replace('{date}', job.date)
    sql_content = re.sub(r'\{date([+\-])([0-9]+)([d])\}', date_expr(job.date), sql_content)
    if isinstance(params, dict):
        for k, v in params.items():
            sql_content = sql_content.replace('{' + k + '}', str(v))
    return sql_content


def date_expr(date: str) -> Callable[[Match], str]:
    """替换日期参数"""
    def date_expr_internal(mo: Match) -> str:
        days = int(mo.group(2))
        if mo.group(1) == '-':
            days = -days
        new_date = datetime.strptime(date, '%Y%m%d') + timedelta(days=days)
        return new_date.strftime('%Y%m%d')
    return date_expr_internal


def collect_tmp_tables(tmp_tables: Set[str], sql: str):
    """收集需要删除的临时表"""
    mo = re.match(r'^(?i)DROP\s+TABLE\s+IF\s+EXISTS\s+(.*)$', sql)
    if mo is not None:
        tmp_tables.add(mo.group(1))


def drop_tmp_tables(engine, tmp_tables: Iterable[str]):
    """删除临时表"""
    for chunk in chunks(list(tmp_tables), 5):
        run_sql(engine, f'DROP TABLE IF EXISTS {", ".join(chunk)}')


def chunks(data: List[T], n: int) -> Iterator[List[T]]:
    """
    Yield successive n-sized chunks from data.
    :param data: list
    :param n: chunk size
    """
    assert isinstance(data, list)
    for i in range(0, len(data), n):
        yield data[i:i + n]
