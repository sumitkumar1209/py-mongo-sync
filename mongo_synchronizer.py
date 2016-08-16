import datetime
import exceptions
import logging
import multiprocessing
import sys
import time

import bson
import pymongo
from pymongo import ReplaceOne

import filter
import mongo_helper

exclude_dbnames = ['admin', 'local']


class MongoSynchronizer(object):
    """ MongoDB multi-source synchronizer.
    """

    def __init__(self, src_hostportstr, dst_hostportstr, **kwargs):
        """ Constructor.
        """
        if not src_hostportstr:
            raise Exception('src hostportstr is empty')
        if not dst_hostportstr:
            raise Exception('dst hostportstr is empty')

        self._src_mc = None
        self._dst_mc = None
        self._filter = None
        self._query = None
        self._w = 1  # write concern, default 1
        self._start_optime = None  # if true, only sync oplog
        self._last_optime = None  # optime of the last oplog has been replayed
        self._logger = logging.getLogger()

        self._ignore_dbs = ['admin', 'local']
        self._ignore_colls = ['system.indexes', 'system.profile', 'system.users']

        self._src_engine = kwargs.get('src_engine')
        self._src_username = kwargs.get('src_username')
        self._src_password = kwargs.get('src_password')
        self._dst_username = kwargs.get('dst_username')
        self._dst_password = kwargs.get('dst_password')
        self._dst_db = kwargs.get('g_ddb')
        self._dst_coll = kwargs.get('dst_coll')
        collections = kwargs.get('collections')
        self._ignore_indexes = kwargs.get('ignore_indexes')
        self._query = kwargs.get('query', None)
        self._start_optime = kwargs.get('start_optime')
        self._w = kwargs.get('write_concern', 1)

        if collections:
            self._filter = filter.CollectionFilter()
            self._filter.add_target_collections(collections)

        # init src mongo client
        self._src_host = src_hostportstr.split(':')[0]
        self._src_port = int(src_hostportstr.split(':')[1])
        self._src_mc = mongo_helper.mongo_connect(
            self._src_host,
            self._src_port,
            username=self._src_username,
            password=self._src_password,
            w=self._w)

        # init dst mongo client
        self._dst_host = dst_hostportstr.split(':')[0]
        self._dst_port = int(dst_hostportstr.split(':')[1])
        self._dst_mc = mongo_helper.mongo_connect(
            self._dst_host,
            self._dst_port,
            username=self._dst_username,
            password=self._dst_password,
            w=self._w)

    def __del__(self):
        """ Destructor.
        """
        if self._src_mc:
            self._src_mc.close()
        if self._dst_mc:
            self._dst_mc.close()

    def _sync(self):
        """ Sync databases and oplog.
        """
        if self._start_optime:
            self._logger.info("locating oplog, it will take a while")
            if self._src_engine == 'mongodb':
                oplog_start = bson.timestamp.Timestamp(int(self._start_optime), 0)
                doc = self._src_mc['local']['oplog.rs'].find_one({'ts': {'$gte': oplog_start}})
                if not doc:
                    self._logger.error('specified oplog not found')
                    return
                oplog_start = doc['ts']
            elif self._src_engine == 'tokumx':
                timestamp_str = self._start_optime
                if len(timestamp_str) != 14:
                    self._logger.error(
                        "invalid --start-optime, start optime for TokuMX should be 14 characters, like 'YYYYmmddHHMMSS'")
                    return
                oplog_start = datetime.datetime(int(timestamp_str[0:4]), int(timestamp_str[4:6]),
                                                int(timestamp_str[6:8]),
                                                int(timestamp_str[8:10]), int(timestamp_str[10:12]),
                                                int(timestamp_str[12:14]))
                doc = self._src_mc['local']['oplog.rs'].find_one({'ts': {'$gte': oplog_start}})
                if not doc:
                    self._logger.error('specified oplog not found')
                    return
                oplog_start = doc['ts']
            self._logger.info('start timestamp is %s actually' % oplog_start)
            self._last_optime = oplog_start
            self._sync_oplog(oplog_start)
        else:
            oplog_start = None
            if self._src_engine == 'mongodb':
                oplog_start = mongo_helper.get_optime(self._src_mc)
            elif self._src_engine == 'tokumx':
                oplog_start = mongo_helper.get_optime_tokumx(self._src_mc)
            if not oplog_start:
                self._logger.error('[%s] get oplog_start failed, terminate' % self._current_process_name)
                sys.exit(1)
            self._last_optime = oplog_start
            self._sync_databases()
            self._sync_oplog(oplog_start)

    def _sync_databases(self):
        """ Sync databases except 'admin' and 'local'.
        """
        host, port = self._src_mc.primary
        self._logger.info('[%s] sync databases from %s:%d' % (self._current_process_name, host, port))
        for dbname in self._src_mc.database_names():
            if dbname not in exclude_dbnames:
                if self._filter and not self._filter.valid_database(dbname):
                    continue
                self._sync_database(dbname)
        self._logger.info('[%s] all databases done' % self._current_process_name)

    def _sync_database(self, dbname):
        """ Sync a database.
        """
        # Q: Why create indexes first?
        # A: It may occured that create indexes failed after you have imported the data,
        #    for example, when you create an unique index without 'dropDups' option and get a 'duplicate keys' error.
        #    Because when you export and import data, duplicate key document may be produced.
        #    Another reason is for TokuMX. It does not support 'dropDups' option for an uniuqe index.
        #    The 'duplicate keys' error must cause index creation failed.
        self._logger.info("[%s] sync database '%s'" % (self._current_process_name, dbname))
        self._sync_indexes(dbname)
        self._sync_collections(dbname)

    def _sync_collections(self, dbname):
        """ Sync all collections in the database except system collections.
        """
        collnames = self._src_mc[dbname].collection_names(include_system_collections=False)
        for collname in collnames:
            if self._filter and not self._filter.valid_collection('%s.%s' % (dbname, collname)):
                continue
            if collname in self._ignore_colls:
                continue
            self._sync_collection(dbname, collname)

    def _sync_collection(self, dbname, collname):
        """ Sync a collection through batch write.
        """
        self._logger.info("[%s] sync collection '%s.%s'" % (self._current_process_name, dbname, collname))
        while True:
            try:
                n = 0
                # docs = []
                reqs = []
                batchsize = 1000
                cursor = self._src_mc[dbname][collname].find(filter=self._query,
                                                             cursor_type=pymongo.cursor.CursorType.EXHAUST,
                                                             no_cursor_timeout=True,
                                                             modifiers={'$snapshot': True})
                count = cursor.count()
                if count == 0:
                    self._logger.info('[%s] \t skip empty collection' % (self._current_process_name))
                    return
                for doc in cursor:
                    reqs.append(ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
                    if len(reqs) == batchsize:
                        self._bulk_write(self._dst_db or dbname, self._dst_coll or collname, reqs, ordered=False)
                        reqs = []
                    n += 1
                    if n % 10000 == 0:
                        self._logger.info('[%s] \t %s.%s %d/%d (%.2f%%)' % (
                            self._current_process_name, dbname, collname, n, count, float(n) / count * 100))
                if len(reqs) > 0:
                    self._bulk_write(self._dst_db or dbname, self._dst_coll or collname, reqs, ordered=False)
                self._logger.info('[%s] \t %s.%s %d/%d (%.2f%%)' % (
                    self._current_process_name, dbname, collname, n, count, float(n) / count * 100))
                return
            except pymongo.errors.AutoReconnect:
                self._src_mc.close()
                self._src_mc = self.reconnect(self._src_host,
                                              self._src_port,
                                              username=self._src_username,
                                              password=self._src_password,
                                              w=self._w)

    def _sync_indexes(self, dbname):
        """ Create indexes.
        """

        def format(key_direction_list):
            """ Format key and direction of index.
            """
            res = []
            for key, direction in key_direction_list:
                if isinstance(direction, float) or isinstance(direction, long):
                    direction = int(direction)
                res.append((key, direction))
            return res

        if self._ignore_indexes:
            return

        for collname in self._src_mc[dbname].collection_names():
            if self._filter and not self._filter.valid_index('%s.%s' % (dbname, collname)):
                continue
            if collname in self._ignore_colls:
                continue
            index_info = self._src_mc[dbname][collname].index_information()
            for index in index_info.itervalues():
                keys = index['key']
                options = {}
                if 'unique' in index:
                    options['unique'] = index['unique']
                if 'sparse' in index:
                    options['sparse'] = index['sparse']
                if 'expireAfterSeconds' in index:
                    options['expireAfterSeconds'] = index['expireAfterSeconds']
                if 'partialFilterExpression' in index:
                    options['partialFilterExpression'] = index['partialFilterExpression']
                if 'dropDups' in index:
                    options['dropDups'] = index['dropDups']
                # create indexes before import documents, ignore 'background' option
                # if 'background' in index:
                #    options['background'] = index['background']
                self._dst_mc[self._dst_db or dbname][self._dst_coll or collname].create_index(format(keys), **options)

    def _sync_oplog(self, oplog_start):
        """ Replay oplog.
        """
        try:
            host, port = self._src_mc.primary
            self._logger.info('try to sync oplog from %s on %s:%d' % (oplog_start, host, port))
            cursor = self._src_mc['local']['oplog.rs'].find({'ts': {'$gte': oplog_start}},
                                                            cursor_type=pymongo.cursor.CursorType.TAILABLE,
                                                            no_cursor_timeout=True)
            if cursor[0]['ts'] != oplog_start:
                self._logger.error('%s is stale, terminate' % oplog_start)
                return
        except IndexError as e:
            self._logger.error(e)
            self._logger.error('%s not found, terminate' % oplog_start)
            return
        except Exception as e:
            self._logger.error(e)
            raise e

        self._logger.info('replaying oplog')
        n_replayed = 0
        while True:
            try:
                if not cursor.alive:
                    self._logger.error('cursor is dead')
                    raise pymongo.errors.AutoReconnect

                # get an oplog
                oplog = cursor.next()

                # validate oplog
                if self._filter and not self._filter.valid_oplog(oplog):
                    continue

                # guarantee that replay oplog successfully
                recovered = False
                while True:
                    try:
                        if recovered:
                            self._logger.info('recovered at %s' % oplog['ts'])
                            recovered = False
                        self._replay_oplog(oplog)
                        n_replayed += 1
                        if n_replayed % 1000 == 0:
                            self._print_progress(oplog)
                        break
                    except pymongo.errors.DuplicateKeyError as e:
                        # TODO
                        # through unique index, delete old, insert new
                        # self._logger.error(oplog)
                        # self._logger.error(e)
                        break
                    except pymongo.errors.AutoReconnect as e:
                        self._logger.error(e)
                        self._logger.error('interrupted at %s' % oplog['ts'])
                        self._dst_mc = self.reconnect(
                            self._dst_host,
                            self._dst_port,
                            username=self._dst_username,
                            password=self._dst_password,
                            w=self._w)
                        if self._dst_mc:
                            recovered = True
                    except pymongo.errors.WriteError as e:
                        self._logger.error(e)
                        self._logger.error('interrupted at %s' % oplog['ts'])
                        self._dst_mc = self.reconnect(
                            self._dst_host,
                            self._dst_port,
                            username=self._dst_username,
                            password=self._dst_password,
                            w=self._w)
                        if self._dst_mc:
                            recovered = True
            except StopIteration as e:
                # there is no oplog to replay now, wait a moment
                time.sleep(0.1)
            except pymongo.errors.AutoReconnect:
                self._src_mc.close()
                self._src_mc = self.reconnect(
                    host,
                    port,
                    username=self._src_username,
                    password=self._src_password,
                    w=self._w)
                cursor = self._src_mc['local']['oplog.rs'].find({'ts': {'$gte': self._last_optime}},
                                                                cursor_type=pymongo.cursor.CursorType.TAILABLE,
                                                                no_cursor_timeout=True)
                if cursor[0]['ts'] != self._last_optime:
                    self._logger.error('%s is stale, terminate' % self._last_optime)
                    return

    def _replay_oplog(self, oplog):
        if self._src_engine == 'mongodb':
            self._replay_oplog_mongodb(oplog)
        elif self._src_engine == 'tokumx':
            self._replay_oplog_tokumx(oplog)

    def _print_progress(self, oplog):
        ts = oplog['ts']
        if self._src_engine == 'mongodb':
            self._logger.info('sync to %s, %s' % (datetime.datetime.fromtimestamp(ts.time), ts))
        elif self._src_engine == 'tokumx':
            self._logger.info('sync to %s' % ts)

    def _replay_oplog_mongodb(self, oplog):
        """ Replay oplog on destination if source is MongoDB.
        """
        # parse
        ts = oplog['ts']
        op = oplog['op']  # 'n' or 'i' or 'u' or 'c' or 'd'
        ns = oplog['ns']
        dbname = ns.split('.', 1)[0]
        if dbname not in exclude_dbnames:
            if not self._filter or self._filter.valid_database(dbname):
                return
            if op in ['i', 'u', 'd']:
                # coll based operation
                collname = ns.split('.', 1)[1]
                if collname not in self._ignore_colls:
                    if not self._filter or self._filter.valid_collection('%s.%s' % (dbname, collname)):
                        dbname = self._dst_db or dbname
                        collname = self._dst_coll or collname
                        if op == 'i':  # insert
                            self._dst_mc[dbname][collname].insert_one(oplog['o'])
                        elif op == 'u':  # update
                            self._dst_mc[dbname][collname].update(oplog['o2'], oplog['o'])
                        elif op == 'd':  # delete
                            self._dst_mc[dbname][collname].delete_one(oplog['o'])
            elif op == 'c':  # command
                dbname = self._dst_db or dbname
                self._dst_mc[dbname].command(oplog['o'])
            elif op == 'n':  # no-op
                pass
            else:
                self._logger.error('unknown operation: %s' % oplog)
        self._last_optime = ts

    def _replay_oplog_tokumx(self, oplog):
        """ Replay oplog on destination if source is TokuMX.
        """
        for op in oplog['ops']:
            if op['op'] == 'i':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                self._dst_mc[dbname][collname].insert_one(op['o'])
            elif op['op'] == 'u':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                self._dst_mc[dbname][collname].update({'_id': op['o']['_id']}, op['o2'])
            elif op['op'] == 'ur':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                self._dst_mc[dbname][collname].update({'_id': op['pk']['']}, op['m'])
            elif op['op'] == 'd':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                self._dst_mc[dbname][collname].remove({'_id': op['o']['_id']})
            elif op['op'] == 'c':
                dbname = op['ns'].split('.', 1)[0]
                self._dst_mc[dbname].command(op['o'])
            elif op['op'] == 'n':
                pass
            else:
                self._logger.error('unknown operation: %s' % op)
        self._last_optime = oplog['ts']

    @property
    def _current_process_name(self):
        return multiprocessing.current_process().name

    def run(self):
        """ Start data synchronization.
        """
        # never drop database automatically
        # you should clear the databases manually if necessary
        try:
            self._sync()
        except exceptions.KeyboardInterrupt:
            self._logger.info('terminating')

    def reconnect(self, host, port, **kwargs):
        """ Try to reconnect until success.
        """
        while True:
            try:
                self._logger.info('try to reconnect %s:%d' % (host, port))
                mc = mongo_helper.mongo_connect(host, port, **kwargs)
            except:
                pass
            else:
                return mc

    def _bulk_write(self, dbname, collname, requests, ordered=True, bypass_document_validation=False):
        """ Try to bulk write until success.
        """
        while True:
            try:
                self._dst_mc[dbname][collname].bulk_write(requests,
                                                          ordered=ordered,
                                                          bypass_document_validation=bypass_document_validation)
            except pymongo.errors.AutoReconnect:
                self._dst_mc.close()
                self._dst_mc = self.reconnect(self._dst_host,
                                              self._dst_port,
                                              username=self._dst_username,
                                              password=self._dst_password,
                                              w=self._w)
            else:
                return
