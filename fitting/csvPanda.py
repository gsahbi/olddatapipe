import logging
import pandas as pd
from . import fitting


class csvPanda(fitting):
    def __init__(self, spec):
        super().__init__(spec)
        if 'dialect' in spec and type(self.spec['dialect']) == dict:
            self.__dialect = spec['dialect']
            # TODO validate dialect parameters
        else:
            self.__dialect = {
                'sep': ',',
                'header': 0
            }
        self.dtype = None
        if 'dtype' in spec:
            self.dtype = self.__parse_dtype(spec['dtype'])

    def __parse_dtype(self, dtype):
        out = {}
        for op in dtype:
            if type(op) != dict or {'column', 'type'} <= set(op):
                logging.error("Ingnoring malformed dtype " + str(op))
            m = op['column']
            v = op['type']
            if v == 'str':
                v = str
            elif v == 'int':
                v = int
            elif v == 'float':
                v = float
            out[m] = v
        return out

    def _process(self, data):
        logging.info('Processing data at ' + self.__class__.__name__)
        meta, _file = data

        df = pd.read_csv(_file,
                         sep=self.__dialect['sep'],
                         header=self.__dialect['header'],
                         dtype=self.dtype
                         )

        yield meta, df
