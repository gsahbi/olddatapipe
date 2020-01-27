import logging
import re
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

    def __parse_replace(self, remap):
        out = {}
        for op in remap:
            if type(op) != dict or {'match', 'value'} <= set(op):
                logging.error("Ingnoring malformed map " + str(op))
            m = op['match']
            v = op['value']
            #if type(m) == str and m[0] == m[-1] == '/' :
            out[m] = v
        return out

    def __load_tidy_spec(self, spec):
        if type(spec) != list:
            logging.error("Tidy spec must be a list of operations to a table.")
            return

        for p in spec:
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

            elif fname == "group_by":
                def group_by(df, columns=None, function='sum'):
                    cols = get_cols(columns, df.columns)
                    if cols is None:
                        raise ValueError("Bad parameter 'columns' in group_by " + str(columns))
                    try:
                        df = df.groupby(cols, axis=0, as_index=False, squeeze=True).sum()
                    except Exception as e:
                        raise RuntimeError("Runtime Error processing group_by operation on Dataframe." + e)
                    return df

                self.__tidy.append((fname, Func(group_by, args)))

            elif fname == "dropcol":
                def drop_col(df, columns=None):
                    cols = get_cols(columns, df.columns)
                    if cols is None:
                        raise ValueError("Bad parameter 'columns' in drop_col " + str(columns))
                    try:
                        df.drop(cols, axis=1, inplace=True)
                    except Exception as e:
                        raise RuntimeError("Runtime Error processing drop_col operation on Dataframe." + srt(e))
                    return df

                self.__tidy.append((fname, Func(drop_col, args)))

            elif fname == "replace":
                def replace(df, column=None, new_column=None, regex=False, remap={}):
                    col = get_cols(column, df.columns)
                    if col is None:
                        logging.error("Bad or missing parameter 'column' = %s in map " % str(column))
                        return df
                    inplace = new_column is None
                    try:
                        if not inplace:
                            df[new_column] = df[col]
                        else:
                            new_column = col
                        df.replace({new_column:remap}, regex=True, inplace=True)
                        if not inplace:
                            # unchanged values will be replaced by NaN
                            df.loc[df[col] == df[new_column], new_column] = None

                    except Exception as e:
                        raise RuntimeError("Runtime Error processing replace operation on Dataframe." + str(e))
                    return df
                if "remap" in args:
                    if type(args["remap"]) == dict:
                        args["remap"] = [args["remap"]]
                    elif type(args["remap"]) != list:
                        raise ValueError("tidyPanda.replace : remap should be a mapping definition "
                                         "as key:value dict or an array of definitions")
                else:
                    raise ValueError("tidyPanda.replace : remap definition is mandatory")
                args["remap"] = self.__parse_replace(args["remap"])
                self.__tidy.append((fname, Func(replace, args)))

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
