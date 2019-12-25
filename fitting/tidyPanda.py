import logging
from collections import defaultdict

import pandas as pd

from utils import get_cols
from . import fitting


class Func(object):
    def __init__(self, function, kwargs={}):
        self.__kwargs = kwargs.copy()
        self.function = function

    def __call__(self, *args):
        return self.function(*args, **self.__kwargs)


class tidyPanda(fitting):
    def __init__(self, spec):
        super().__init__(spec)

        self.__tidy = []
        self.__load_tidy_spec(spec)

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
                self.__tidy.append((fname, Func(lambda df: df.dropna())))
            elif fname == "fix_column_names":
                def fix_column_names(df):
                    df.columns = df.columns.str.strip()\
                                            .str.lower()\
                                            .str.replace(' ', '_')\
                                            .str.replace('(', '')\
                                            .str.replace(')', '')
                    return df

                self.__tidy.append((fname, Func(fix_column_names)))
            elif fname == "melt":

                def melt(df, **kwargs):
                    if 'id_vars' in kwargs:
                        kwargs['id_vars'] = get_cols(kwargs['id_vars'], df.columns)
                    return pd.melt(df, **kwargs)

                if 'id_vars' in args:
                    if type(args['id_vars']) == int:
                        args['id_vars'] = [args['id_vars']]
                    elif type(args['id_vars']) != list:
                        logging.error("Bad id_vars parameter " + str(args['id_vars']))
                        continue

                self.__tidy.append((fname, Func(melt, args)))

            elif fname == "split":
                def split(df, col, new_cols, sep=',', collapse=None, replace=True):
                    col = get_cols(col, df.columns)
                    if col is None:
                        raise ValueError("Bad parameter 'col' in split " + str(col))

                    try:
                        n_new_cols = len(new_cols)
                        tmp = df[col].copy().str.split(sep, expand=True)
                        n_split_cols = len(tmp.columns)
                        # if resulting columns is different
                        if n_split_cols <= n_new_cols:
                            logging.warning("Unexpected format, found only %d/%d columns. "
                                            "Will adjust to lowest" %(n_split_cols, n_new_cols))
                            new_cols = new_cols[:min(n_split_cols, n_new_cols)]
                            df[new_cols] = tmp
                        else:
                            if collapse is None:
                                logging.warning("Unexpected format, found %d columns expected %d."
                                                "Abandoning .. please configure a collapse column"
                                                % (n_split_cols, n_new_cols))
                                return None

                            # re-collapse all columns starting from collapse to before last
                            tmp['n_cols'] = tmp.notnull().sum(axis=1)
                            cols = tmp.columns.tolist()
                            for n in range(n_new_cols, n_split_cols):
                                tmp.loc[tmp.n_cols == n+1, cols[collapse-1]] = tmp[tmp.n_cols == n+1][cols[collapse-1:n]].apply(sep.join, axis=1)
                                tmp.loc[tmp.n_cols == n+1, cols[collapse]] = tmp[cols[n]]

                            # remove trailing columns
                            tmp.drop(tmp.columns[n_new_cols:], axis=1, inplace=True)
                            df[new_cols] = tmp
                        if replace:
                            df.drop(col, axis=1, inplace=True)
                    except Exception as e:
                        raise RuntimeError("Runtime Error processing split operation on Dataframe." + str(e))
                        return None

                    return df

                if 'col' not in args or 'new_cols' not in args:
                    logging.error("did not specify the target col and new cols")
                    continue

                if 'collapse' in args:
                    _collapse = args['collapse']

                    if type(_collapse) != int:
                        raise ValueError("Bad parameter 'collapse' : %s, should be integer" % str(_collapse))

                    if type(_collapse) != int or 1 > _collapse >= len(args['new_cols']):
                        raise ValueError("Bad parameter 'collapse' : %s, should be within new_cols range" % str(_collapse))

                self.__tidy.append((fname, Func(split, args)))

            elif fname == "pivot":
                def pivot(df, columns, values, index=None):
                    idx = get_cols(index, df.columns)
                    if idx is None:
                        raise ValueError("No 'index' in pivot " + str(index))
                    cols = get_cols(columns, df.columns)
                    if cols is None:
                        raise ValueError("Bad parameter 'columns' in pivot " + str(columns))
                    vals = get_cols(values, df.columns)
                    if vals is None:
                        raise ValueError("Bad parameter 'values' in pivot " + str(values))

                    try:
                        df = pd.pivot_table(df, values=vals, index=idx, columns=cols)
                        df.reset_index(inplace=True)
                    except Exception as e:
                        raise RuntimeError("Runtime Error processing pivot operation on Dataframe." + e)

                    return df

                if 'columns' not in args or 'values' not in args:
                    logging.error("did not specify pivot 'columns' and 'values'")
                    continue
                self.__tidy.append((fname, Func(pivot, args)))

    def _process(self, data):
        logging.info('Processing data at ' + self.__class__.__name__)
        meta, df = data

        # apply tidy functions
        try:
            for k, f in self.__tidy:
                df = f(df)
                logging.info("Applied step [%s] to data frame" % k)
        except Exception as e:
            raise e
        yield meta, df
