#! /usr/bin/env python
# -*- coding: utf-8 -*-

# summary: MongoDB sync tool
# author: caosiyang
# date: 2013/09/16

import argparse
import logging
import logging.handlers
import sys

import pymongo

import mongo_synchronizer

# global variables
g_src = ''
g_src_engine = 'mongodb'
g_src_username = ''
g_src_password = ''
g_dst = ''
g_dst_username = ''
g_dst_password = ''
g_db = ''
g_coll = ''
g_ddb = ''
g_dcoll = ''
g_query = None
g_start_optime = ''
g_write_concern = 1
g_logfilepath = ''


def parse_args():
    """ Parse arguments.
    """
    global g_src, g_src_engine, g_src_username, g_src_password, g_dst, g_dst_username, g_dst_password, g_db, g_coll, g_ddb, g_dcoll, g_query, g_start_optime, g_write_concern, g_logfilepath

    parser = argparse.ArgumentParser(
        description='Sync data from a replica-set to another mongod/replica-set/sharded-cluster.')
    parser.add_argument('--from', nargs='?', required=True, help='the source must be a mongod instance of replica-set')
    parser.add_argument('--src-username', nargs='?', required=False, help='src username')
    parser.add_argument('--src-password', nargs='?', required=False, help='src password')
    parser.add_argument('--src-engine', nargs='?', required=False,
                        help='src engine, the value could be mongodb or tokumx, default is mongodb')
    parser.add_argument('--to', nargs='?', required=True, help='the destionation should be a mongos or mongod instance')
    parser.add_argument('--dst-username', nargs='?', required=False, help='dst username')
    parser.add_argument('--dst-password', nargs='?', required=False, help='dst password')
    parser.add_argument('--sdb', nargs='?', required=False, help='source database to sync')
    parser.add_argument('--scoll', nargs='?', required=False, help='source collection to sync')
    parser.add_argument('--ddb', nargs='?', required=False, help='destination database to sync')
    parser.add_argument('--dcoll', nargs='?', required=False, help='destination collection to sync')
    parser.add_argument('--query', nargs='?', required=False, help='json query')
    parser.add_argument('--start-optime', nargs='?', required=False,
                        help="start optime, a timestamp value in second for MongoDB or a 'YYYYmmddHHMMSS' value for TokuMX")
    parser.add_argument('--write-concern', nargs='?', required=False, help='write concern, default 1')
    parser.add_argument('--log', nargs='?', required=False, help='log file path')
    # parser.add_argument('--oplog', action='store_true', help='enable continuous synchronization')

    args = vars(parser.parse_args())
    if args['from']:
        g_src = args['from']
    if args['src_engine']:
        if args['src_engine'] not in ['mongodb', 'tokumx']:
            print 'invalid src_engine, terminate'
            sys.exit(1)
        g_src_engine = args['src_engine']
    if args['src_username']:
        g_src_username = args['src_username']
    if args['src_password']:
        g_src_password = args['src_password']
    if args['to']:
        g_dst = args['to']
    if args['dst_username']:
        g_dst_username = args['dst_username']
    if args['dst_password']:
        g_dst_password = args['dst_password']
    if args['sdb']:
        g_db = args['sdb']
    if args['scoll']:
        g_coll = args['scoll']
    if args['ddb']:
        g_ddb = args['ddb']
    if args['dcoll']:
        g_dcoll = args['dcoll']
    if args['query']:
        g_query = eval(args['query'])
    if args['start_optime']:
        g_start_optime = args['start_optime']
    if args['write_concern']:
        g_write_concern = int(args['write_concern'])
    if args['log']:
        g_logfilepath = args['log']


def logger_init(filepath):
    """ Init logger for global.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    if filepath:
        handler_log = logging.handlers.RotatingFileHandler(filepath, mode='a', maxBytes=1024 * 1024 * 100,
                                                           backupCount=3)
        handler_log.setFormatter(formatter)
        handler_log.setLevel(logging.INFO)
        logger.addHandler(handler_log)
    else:
        handler_stdout = logging.StreamHandler(sys.stdout)
        handler_stdout.setFormatter(formatter)
        handler_stdout.setLevel(logging.INFO)
        logger.addHandler(handler_stdout)


def main():
    global g_src, g_src_engine, g_src_username, g_src_password, g_dst, g_dst_username, g_dst_password, g_db, g_coll, g_ddb, g_dcoll, g_query, g_start_optime, g_write_concern, g_logfilepath

    parse_args()

    logger_init(g_logfilepath)
    logger = logging.getLogger()
    logger.info('================================================')
    logger.info('src             :  %s' % g_src)
    logger.info('src engine      :  %s' % g_src_engine)
    logger.info('src username    :  %s' % g_src_username)
    logger.info('src password    :  %s' % g_src_password)
    logger.info('dst             :  %s' % g_dst)
    logger.info('dst username    :  %s' % g_dst_username)
    logger.info('dst password    :  %s' % g_dst_password)
    logger.info('src database    :  %s' % g_db)
    logger.info('src collection  :  %s' % g_coll)
    logger.info('dst database    :  %s' % g_ddb)
    logger.info('dst collection  :  %s' % g_dcoll)
    logger.info('query           :  %s' % g_query)
    logger.info('start optime    :  %s' % g_start_optime)
    logger.info('write concern   :  %s' % g_write_concern)
    logger.info('log filepath    :  %s' % g_logfilepath)
    logger.info('pymongo version :  %s' % pymongo.version)
    logger.info('================================================')

    colls = []
    if g_db and g_coll:
        colls.append('%s.%s' % (g_db, g_coll))

    syncer = mongo_synchronizer.MongoSynchronizer(
        g_src,
        g_dst,
        src_username=g_src_username,
        src_password=g_src_password,
        src_engine=g_src_engine,
        dst_username=g_dst_username,
        dst_password=g_dst_password,
        dst_db=g_ddb,
        dst_coll=g_dcoll,
        collections=colls,
        ignore_indexes=False,
        query=g_query,
        start_optime=g_start_optime,
        write_concern=g_write_concern)
    syncer.run()
    logger.info('exit')


if __name__ == '__main__':
    main()
