import logging

from sqlalchemy import text

from kiwi import app, db

logger = logging.getLogger(__name__)


class Job:
    def __init__(self, date: str, suffix: str):
        self.date = date
        self.suffix = suffix

    def run(self) -> None:
        print(self.date, self.suffix, app.config['fp'])

        result = db.session('dw').scalar(text('SELECT COUNT(*) FROM zj_tmp1'))
        logger.info('Result: %d', result)
