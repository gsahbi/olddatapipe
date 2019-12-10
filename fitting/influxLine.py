import logging
import re
from collections import defaultdict
from datetime import datetime

from . import fitting

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
        self.__fields = self.__parse_col_spec(spec['fields'])

        if 'tags' in spec:
            self.__tags = self.__parse_col_spec(spec['tags'])
        else:
            self.__tags = []

    # helper method to convert to a line protocol representation
    def to_line(self, name, ts, tags: dict, fields: dict):
        out = name
        if len(tags):
            out = out + "," + ",".join(["%s=%s" % (str(a), str(b)) for a, b in tags.items()])
        out = out + " " + ",".join(["%s=%s" % (str(a), str(b)) for a, b in fields.items()]) + " " + str(ts)
        return out

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

    def __parse_col_spec(self, spec):

        f = []
        cols = spec
        if type(cols) != list:
            cols = [cols]

        for col in cols:

            if type(col) == str or type(col) == int:
                col = {'column': col}

            if type(col) == dict:
                if 'column' not in col:
                    logging.error("Field does not contain a mandatory 'column' value or index " + str(col))
                    continue
            else:
                logging.error("Error parsing given field" + str(col))
                continue

            if 'name' not in col:
                if type(col['column']) == str:
                    col['name'] = col['column']
                elif type(col['column']) == int:
                    col['name'] = 'column_%d' % col['column']
                else:
                    logging.error("Column type must be a name or an index" + str(col['column']))
                    continue

            f.append(col)
        return f

    def _process(self, _data):
        logging.info('Processing data at ' + self.__class__.__name__)
        meta, res = _data

        # process fields
        field_values = {}
        for field in self.__fields:
            col = field['column']
            if 'name_from' in field:
                name_from = field["name_from"]
                if type(name_from) == int:
                    if name_from > len(res) or col < 0:
                        logging.error("Field column index %s not found!" % name_from)
                        continue
                    name = list(res.items())[name_from-1][1]
                elif col in res:
                    name = res[name_from]
                else:
                    logging.error("Field name extraction column %s not found !" % col)
            else:
                name = str(field['name'])

            if type(col) == int:
                if col > len(res) or col < 0:
                    logging.error("Field column index %s not found!" % col)
                    continue
                val = list(res.items())[col-1][1]
            elif col in res:
                val = res[col]
            else:
                logging.error("Field column name %s not found !" % col)

            name = re.escape(name)
            if type(val) == str:
                val = re.escape(val)

            field_values[name] = val

        # process tags
        tag_values = {}
        for tag in self.__tags:
            col = tag['column']
            name = str(tag['name'])

            if type(col) == int:
                if abs(col) >= len(res):
                    logging.error("Tag column index %s not found!" % col)
                    continue
                val = list(res.items())[col-1][1]
            elif col in res:
                val = res[col]
            else:
                logging.error("Tag column name %s not found !" % col)

            name = re.escape(name)
            if type(val) == str:
                val = re.escape(val)

            tag_values[name] = val

        # process timestamp
        ts = int(datetime.now().timestamp())

        if self.__ts['method'] == TS_FILE_MODIFIED and 'ts' in meta:
            ts = meta['ts']
        elif self.__ts['method'] == TS_FILE_NAME and 'name' in meta:
            # extract date time from filename
            s = self.__ts['method'].search(meta['name']).group(1)

            try:
                d = datetime.strptime(s, self.__ts['format'])
                ts = int(d.timestamp())
            except ValueError as e:
                logging.error(e)

        elif self.__ts['method'] == TS_COLUMN:
            col = self.__ts['column']
            if type(col) == int:
                if col > len(res) or col < 0:
                    logging.error("TS column index %s out of range !" % col)
                else:
                    d = list(res.items())[col-1][1]
                    if type(d) == str:
                        try:
                            s = datetime.strptime(d, self.__ts['format'])
                            ts = int(s.timestamp() * 10e6)
                        except ValueError as e:
                            logging.error(e)
                    else:
                        ts = int(d * 10e6)

            elif type(col) == str and col in res:
                d = res[col]
                if type(d) == str:
                    try:
                        s = datetime.strptime(d, self.__ts['format'])
                        ts = int(s.timestamp() * 10e6)
                    except ValueError as e:
                        logging.error(e)
                else:
                    ts = int(d * 10e6)
            else:
                logging.error("TS column name %s not found !" % col)


        else:
            logging.error("Couldn't extract timestamp of measurement")

        print(self.to_line(self.__measurement, ts, tag_values, field_values))
        yield None
