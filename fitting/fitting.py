import abc
import logging


class fitting(metaclass=abc.ABCMeta):

    def __init__(self, _):
        self.__next = None

    @abc.abstractmethod
    def _process(self, data):
        pass

    def hook(self, ftng):
        if ftng is None or not issubclass(type(ftng), fitting):
            logging.fatal("Cannot plumb ")
            return 0
        self.__next = ftng

    def flow(self, datain=None):

        # a generator that yields items instead of returning a list
        for dataout in self._process(datain):
            # pass it to th next fitting
            if self.__next:
                self.__next.flow(dataout)
