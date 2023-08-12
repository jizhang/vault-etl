import logging
import os
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)
db: Any = None


class Application:
    def __init__(self):
        self.setup_logging()
        self.config = self.load_config()

    def setup_logging(self) -> None:
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)

    def load_config(self) -> Dict[str, Any]:
        config_files = ['config.yml']

        if os.environ.get('KIWI_CONFIG'):
            config_files.append(os.environ['KIWI_CONFIG'])

        config: dict = {}
        for config_file in config_files:
            config_file = os.path.abspath(config_file)
            logger.info('Load config from %s', config_file)
            with open(config_file) as f:
                config.update(yaml.safe_load(f))

        return config


app = Application()
