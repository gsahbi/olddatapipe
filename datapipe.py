import logging
import sys

import yaml

from pipeline import pipeline


def main(cfg):
    ls = {}
    done = {}
    iter = None

    # load files and


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)-19.19s %(levelname)s:%(name)s:%(lineno)s: %(message)s')
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    confile = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"

    try:
        with open(confile, 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.Loader)
    except Exception as x:
        logging.error("Failed to read config file " + str(x))
        exit(1)
    p = pipeline(cfg['pipeline'])
    p.flow()
