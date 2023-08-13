import logging

from sqlalchemy import text

from kiwi import app, db, dw

logger = logging.getLogger(__name__)


class Job:
    def __init__(self, date: str, suffix: str):
        self.date = date
        self.suffix = suffix

    def run(self) -> None:
        print(self.date, self.suffix, app.config['fp'])

        dw.run(self, 'dw_fake_job.sql')

        result = db.session('dw').scalar(
            text('SELECT cnt FROM dw_crius_daily WHERE report_date = :date'),
            {'date': self.date},
        )
        logger.info('Result: %d', result)
