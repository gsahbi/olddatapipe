import logging
from fitting import *

from utils import to_camel_case


class pipeline(object):
    def __init__(self, cfg):
        if type(cfg) != list:
            cfg = [cfg]
        self.__cfg = cfg
        self.__nodes = []
        self.__build()

    def __build(self):
        for p in self.__cfg:
            spec = {}
            if type(p) == str:
                # assuming this doesn't require a spec
                classname = to_camel_case(p)
            elif 'kind' in p:
                classname = to_camel_case(p['kind'])
            else:
                logging.error("Don't know how to parse : " + str(p))
                raise ValueError
            if 'spec' in p:
                spec = p['spec']
            try:
                self.__nodes.append(globals()[classname](spec))
            except KeyError as e:
                logging.fatal("Couldn't not create object of class " + classname + str(e))

        # wire them up in sequence only TODO: support complex plumbing
        for o in range(0, len(self.__nodes) - 1):
            self.__nodes[o].hook(self.__nodes[o + 1])

    def flow(self, datain=None):
        return self.__nodes[0].flow(datain)
