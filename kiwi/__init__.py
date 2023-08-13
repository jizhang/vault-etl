import logging
import os
from typing import Any, Dict

import yaml

from .sqlalchemy import SQLAlchemy

logger = logging.getLogger(__name__)
db = SQLAlchemy()


class Application:
    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.debug = False

    def start(self) -> None:
        self.setup_logging()
        self.load_config()
        db.init_app(self)

    def setup_logging(self) -> None:
        logging.basicConfig(
            format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
            level=logging.INFO,
        )

        if app.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


    def load_config(self) -> None:
        config_files = ['config.yml']

        if os.environ.get('KIWI_CONFIG'):
            config_files.append(os.environ['KIWI_CONFIG'])

        for config_file in config_files:
            config_file = os.path.abspath(config_file)
            logger.info('Load config from %s', config_file)
            with open(config_file) as f:
                self.config.update(yaml.safe_load(f))


app = Application()
