import logging
import re
from datetime import datetime

import pandas as pd

from . import fitting
from influxdb import DataFrameClient
from utils import get_cols

TS_FILE_MODIFIED = 1
TS_FILE_NAME = 2
TS_COLUMN = 3


class influxLine(fitting):
    def __init__(self, spec):
        super().__init__(spec)
        if 'fields' not in spec:
            logging.fatal("You must provide a list of fields!")
            raise SyntaxError

        if 'measurement' not in spec:
            logging.fatal("You must provide a measurement name!")
            raise SyntaxError

        if 'ts' not in spec:
            logging.fatal("You should provide a timestamp extraction method!")
            raise SyntaxError

        self.__measurement = spec['measurement']
        self.__parse_ts_spec(spec['ts'])
        self.__fields = spec['fields']
        self.__tags = spec['tags'] if 'tags' in spec else []

    def __parse_ts_spec(self, spec):

        self.__ts = dict()
        # possible values of method : columm, filename, filemodified if present
        if 'method' not in spec:
            logging.error("ts extraction unspecified, defaulting to file modified date if present")
            self.__ts['method'] = TS_FILE_MODIFIED
            return

        self.__ts = spec.copy()
        if self.__ts['method'] == 'column':
            if 'column' not in self.__ts or 'format' not in self.__ts:
                logging.error("ts extraction method column requires the properties 'column' and 'format'.")
                raise SyntaxError
            self.__ts['method'] = TS_COLUMN
            return

        if self.__ts['method'] == 'filename':
            if 'regex' not in self.__ts or 'format' not in self.__ts:
                logging.error("ts extraction method column requires the properties 'regex' and 'format'.")
                raise SyntaxError
            # precompile regex
            self.__ts['regex'] = re.compile(self.__ts['regex'])
            self.__ts['method'] = TS_FILE_NAME

    def _process(self, _data):
        logging.info('Processing data at ' + self.__class__.__name__)
        meta, df = _data

        # process fields
        field_columns = get_cols(self.__fields, df.columns)
        if field_columns is None:
            logging.error("Bad parameter 'fields'" + str(self.__fields))
            raise ValueError

        # process tags
        tag_columns = get_cols(self.__tags, df.columns)

        # process timestamp
        if self.__ts['method'] == TS_FILE_MODIFIED and 'ts' in meta:
            df["ts"] = meta['ts']
        elif self.__ts['method'] == TS_FILE_NAME and 'name' in meta:
            # extract date time from filename
            s = self.__ts['method'].search(meta['name']).group(1)
            try:
                df["ts"] = datetime.strptime(s, self.__ts['format'])
            except ValueError as e:
                logging.error(e)

        elif self.__ts['method'] == TS_COLUMN:
            ts_column = get_cols(self.__ts['column'], df.columns)
            if ts_column is None:
                logging.error("Bad parameter ts.column" + str(self.__tags))
                raise ValueError
            df.rename(columns={ts_column: 'ts'}, inplace=True)
            if self.__ts['format'] in ['D', 's', 'ms', 'us', 'ns']:
                df['ts'] = pd.to_datetime(df['ts'], unit=self.__ts['format'])
            else:
                df['ts'] = pd.to_datetime(df['ts'], format=self.__ts['format'])
        else:
            logging.error("Couldn't extract timestamp of measurement, defaulting to now")
            df['ts'] = datetime.now()

        df.set_index('ts', inplace=True)
        lines = DataFrameClient()._convert_dataframe_to_lines(df, self.__measurement,
                                                              field_columns, tag_columns,
                                                              numeric_precision=6)
        # print all lines
        print("\n".join(lines))
        yield None
