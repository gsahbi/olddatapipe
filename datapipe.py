import logging
import sys

import yaml

from pipeline import pipeline

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)-19.19s %(levelname)s:%(name)s:%(lineno)s: %(message)s',
        level=logging.DEBUG)
    # logging.getLogger(__name__).setLevel(logging.DEBUG)
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"

    try:
        with open(config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.Loader)
    except Exception as x:
        logging.error("Failed to read config file " + str(x))
        exit(1)

    exit(pipeline(cfg['pipeline']).run())
