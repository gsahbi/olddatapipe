import logging
from collections import defaultdict

import pandas as pd
from . import fitting


class Func(object):
    def __init__(self, function, kwargs={}):
        self.__kwargs = kwargs.copy()
        self.function = function

    def __call__(self, *args):
        return self.function(*args, **self.__kwargs)


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
        self.__tidy = {}
        if 'tidy' in spec:
            self.__load_tidy_spec(spec['tidy'])

    def __get_cols(self, cols, from_list: list):

        if type(cols) == list:
            out = []
            for col in cols:
                if type(col) == int and len(from_list) >= col > 0:
                    out.append(from_list[col - 1])
                elif type(col) == str and col in from_list:
                    out.append(col)
                else:
                    return None
            return out
        else:
            if type(cols) == int and len(from_list) >= cols > 0:
                return from_list[cols - 1]
            elif type(cols) == str and cols in from_list:
                return cols
            else:
                return None


    def __load_tidy_spec(self, spec):
        if type(spec) != list:
            logging.error("Tidy spec must be a list of operations to a table.")
            return
        for p in spec:
            fname = None
            args = {}

            if type(p) == str:
                fname = p
            elif type(p) == dict and 'kind' in p:
                fname = p['kind']
                if 'spec' in p:
                    args = p['spec']
            else:
                logging.error("Error parsing Tidy operation name")
                continue

            # what operation is required
            if fname == "dropna":
                self.__tidy[fname] = Func(lambda df: df.dropna())
            elif fname == "melt":

                def melt(df, **kwargs):
                    if 'id_vars' in kwargs:
                        kwargs['id_vars'] = self.__get_cols(kwargs['id_vars'], df.columns)
                    return pd.melt(df, **kwargs)

                if 'id_vars' in args:
                    if type(args['id_vars']) == int:
                        args['id_vars'] = [args['id_vars']]
                    elif type(args['id_vars']) != list:
                        logging.error("Bad id_vars parameter " + str(args['id_vars']))
                        continue

                self.__tidy[fname] = Func(melt, args)
            elif fname == "split":
                def split(df, col, new_cols, sep=',', replace=True):
                    col = self.__get_cols(col, df.columns)
                    if col is None:
                        logging.error("Bad parameter 'col' in split " + str(col))
                        raise ValueError

                    df[new_cols] = df[col].str.split(sep, expand=True)
                    if replace:
                        df.drop(col, axis=1, inplace=True)
                    return df

                if 'col' not in args or 'new_cols' not in args:
                    logging.error("did not specify the target col and new cols")
                    continue
                self.__tidy[fname] = Func(split, args)

    def _process(self, data):
        logging.info('Processing data at ' + self.__class__.__name__)
        meta, _file = data

        df = pd.DataFrame()
        # try:
        df = pd.read_csv(_file,
                         sep=self.__dialect['sep'],
                         header=self.__dialect['header'])

        # apply tidy functions
        for k, f in self.__tidy.items():
            df = f(df)
            logging.info("Applied step [%s] to data frame" % k)

        # except Exception as e:
        #     logging.error(e)
        #     raise e

        dd = defaultdict(list)
        ddf = df.to_dict('records', into=dd)
        for row in ddf:
            yield meta, row
