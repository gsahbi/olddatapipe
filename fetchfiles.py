import logging
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


    with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.Loader)
    p = pipeline(cfg['pipeline'])
    p.flow()
    # exit(main(cfg))
