import csv
import logging
import os
from fs import open_fs
from . import fitting


class fsScan(fitting):
    def __init__(self, spec):
        super().__init__(spec)
        assert 'url' in spec
        self.__url = spec['url']
        self.__filter = spec['filter'] if 'filter' in spec else '*'
        self.__walk = spec['walk'] if 'walk' in spec else False
        self.__register = spec['register'] if 'register' in spec else None
        self.__ls = {}

    def _process(self, data):
        # find already processed files
        logging.info('Processing data at ' + self.__class__.__name__)

        done = {}
        if self.__register and os.path.exists(self.__register):
            try:
                with open(self.__register, 'r') as f:
                    for line in csv.reader(f):
                        if len(line) == 2:
                            done[line[1]] = int(line[0])
                        else:
                            logging.warning("Bad line in register file %s : %s" % (self.__register, str(line)))
            except Exception as x:
                logging.error(x)

        with open_fs(self.__url) as home_fs:
            if self.__walk:
                for _p, f in home_fs.walk.info(filter=[self.__filter], namespaces=['details']):
                    if f.is_dir:
                        continue

                    modified = int(f.modified.timestamp())
                    if _p in done:
                        if done[_p] >= modified:
                            continue
                    self.__ls[_p] = modified

            else:
                # list files in the specified path
                iter = home_fs.filterdir('', files=[self.__filter], namespaces=['details'])

                for f in filter(lambda f: not f.is_dir, iter):
                    modified = int(f.modified.timestamp())
                    if f.name in done:
                        if done[f.name] >= modified:
                            continue
                    self.__ls[f.name] = modified

            if len(self.__ls) > 0:
                logging.info("Found %d files." % len(self.__ls))
                for fn, ts in self.__ls.items():
                    try:
                        logging.info("Opening file %s." % fn)
                        with home_fs.open(fn) as _file:
                            meta = {'ts': ts, 'name': fn}
                            yield meta, _file

                        # add to the already done files
                        if self.__register:
                            with open(self.__register, "a") as regf:
                                regf.write("%s,%s\n" % (str(ts), fn))
                    except Exception as x:
                        logging.error("Error processing file %s. %s" % (fn, str(x)))
            else:
                logging.info("No new file found !")
