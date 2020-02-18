import csv
import logging
import os
import time
from fs import open_fs
from . import fitting


class fsScan(fitting):
    def __init__(self, spec):
        super().__init__(spec)
        self.__url = None
        self.__con = None
        if 'url' in spec :
            self.__url = spec['url']
        elif 'connection' in spec and type(spec['connection']) == dict:
            self.__con = spec['connection']
        else:
            raise ValueError("Bad filesystem connection")

        self.__filter = spec['filter'] if 'filter' in spec else '*'
        self.__walk = spec['walk'] if 'walk' in spec else False
        self.__register = spec['register'] if 'register' in spec else None
        self.__retries = int(spec['retries']) if 'retries' in spec else 0
        self.__retry_delay = int(spec['retry_delay']) if 'retry_delay' in spec else 60
        self.__ls = {}

    def _process(self, data):
        # find already processed files
        logging.info('Processing data at ' + self.__class__.__name__)

        done = {}
        retries = self.__retries + 1
        while retries > 0:
            retries -= 1
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

            path = ''
            if self.__url:
                home_fs = open_fs(self.__url)

            elif self.__con:
                c = self.__con
                if c['type'] == 'ssh':
                    from fs.sshfs import SSHFS
                    if 'port'   not in c: c['port'] = 22
                    if 'passwd' not in c: c['passwd'] = None
                    if 'pkey'   not in c: c['pkey'] = None
                    if 'path' in c:  path = c['path']

                    home_fs = SSHFS(host=c['host'],
                                    user=c['user'], passwd=c['passwd'],
                                    pkey=c['pkey'], port=c['port'])

                else:
                    raise ValueError("Unsupported type of connection")


            with home_fs:
                if self.__walk:
                    for _p, f in home_fs.walk.info(path, filter=[self.__filter], namespaces=['details']):
                        if f.is_dir:
                            continue
                        fn = os.path.join(path, _p)
                        modified = int(f.modified.timestamp())
                        if fn in done:
                            if done[fn] >= modified:
                                continue
                        self.__ls[fn] = modified

                else:
                    # list files in the specified path
                    iter = home_fs.filterdir(path, files=[self.__filter], namespaces=['details'])

                    for f in filter(lambda f: not f.is_dir, iter):
                        fn = os.path.join(path, f.name)
                        modified = int(f.modified.timestamp())
                        if fn in done:
                            if done[fn] >= modified:
                                continue
                        self.__ls[fn] = modified

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
                    if self.__retries == 0 :
                        break
                    else:
                        logging.info("Retrying after %d seconds." % self.__retry_delay)
                        time.sleep(self.__retry_delay)
