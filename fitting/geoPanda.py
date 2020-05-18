import logging
import geopandas as gpd
from . import fitting


class geoPanda(fitting):
    def __init__(self, spec):
        super().__init__(spec)
        if 'index' in spec and type(self.spec['index']) == str:
            self.__index = spec['index']

    def _process(self, data):
        logging.info('Processing data at ' + self.__class__.__name__)
        meta, _file = data
        try:
            df = gpd.read_file(_file)
            if self.__index :
                df = df.set_index(self.__index)
            yield meta, df
        except Exception as x:
            logging.error("Error loading geometry file %s." % (_file))
