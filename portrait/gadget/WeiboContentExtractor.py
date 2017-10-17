# -*- coding:utf8 -*-

from portrait.DB.MongodbClient import MongodbClient
from portrait.utils.LogHandler import LogHandler

import json
import sys
import re
import time
from os import listdir
from os.path import isfile,abspath,join
from pymongo import MongoClient

from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
from threading import Lock

logger = LogHandler('content_extractor')

class WeiboContentExtractor(object):

    def __init__(self, host, port, db_name, db_content_collect):
        self.db_content = self.__loadMongodb(host, port, db_name, db_content_collect)

    def __loadMongodb(self, host, port, db_name, db_collect):
        db = MongodbClient()
        db.setDatabase(host, port, db_name, db_collect)
        return db

    def __data2mongo(self, uid, contents):
        if uid:
            u_info = self.db_content.get({'uid':uid})
            if not u_info:
                u_info = {}
            u_info['uid'] = uid
            if 'contents' not in u_info:
                u_info['contents'] = list()
            u_info['contents'].extend(contents)
            self.db_content.update({'uid':uid},u_info)

    def __proc_set_records(self, process_batch_set):
        for uid,contents in process_batch_set.iteritems():
            self.__data2mongo(uid, contents)

    def __is_contain_key(self, dict_set, *args):
        for cond in args:
            if cond not in dict_set:
                return False
        return True

    def __update_dict(self, target_dict, source_dict, *key_args):
        if key_args and isinstance(source_dict, dict) and isinstance(target_dict,dict):
            for key in key_args:
                if key in source_dict and source_dict[key]:
                    target_dict[key] = source_dict[key]
        target_dict['smu_create_time'] = int(time.time())
        return target_dict

    # {uid, [{mid, msg_type, msg_text, text_url, real_time, weibo_url, ip_addr, client_remark, msg_province, msg_city}]}
    def content2mongo(self, data_file, target_uid=set()):
        global logger
        logger.info("start to process %s"%data_file)
        with open(data_file, 'r') as objF:
            over_cnt = 0
            cnt = 0
            process_batch_set = dict()
            for line in objF:
                over_cnt += 1
                cinfo = json.loads(line)
                retry_flag = False  
                if target_uid:
                    if 'uid' not in cinfo or 'msg_type' not in cinfo or cinfo['msg_type']!=3 or cinfo['uid'] not in target_uid:
                        continue
                # keys_exist = self.__is_contain_key('uid', 'mid', 'msg_type', 'msg_text', 'text_url', 'real_time'
                #                 , 'weibo_url', 'ip_addr', 'client_remark', 'msg_province', 'msg_city')
                # if not keys_exist:
                #     continue

                # fetch batch data
                cnt += 1
                extract_cinfo = dict()
                self.__update_dict(extract_cinfo, cinfo, 'mid', 'msg_type', 'msg_text', 'text_url', 'real_time'
                                    , 'weibo_url', 'ip_addr', 'client_remark', 'msg_province', 'msg_city')
                uid = cinfo['uid']
                if uid not in process_batch_set:
                    process_batch_set[uid] = list()
                process_batch_set[uid].append(extract_cinfo)
                # process batch data
                if cnt%1000==0:
                    self.__proc_set_records(process_batch_set)
                    logger.info("Processing %s: #%d(%d) records restored in mongo"%(data_file, cnt, over_cnt))
                    process_batch_set = dict()

            self.__proc_set_records(process_batch_set)
            logger.info(u"Processing %s: #%d(%d) records restored in mongo"%(data_file, cnt, over_cnt))
            logger.info(u"Overall %d records in %s"%(cnt, data_file))

def read_target_list(target_list_file):
    target_list = set()
    with open(target_list_file, 'r') as tlFile:
        for line in tlFile:
            target_list.add(line.strip())
    print 'load %d target uids from %s'%(len(target_list), target_list_file)
    return target_list

def run(msg_dir, process_num=5, target_list_file=''):
    target_list = set()
    if target_list_file:
        target_list = read_target_list(target_list_file)

    ignore_num = 0
    jobs = listdir(msg_dir)
    jobs = jobs[ignore_num:]
    extractor = WeiboContentExtractor('127.0.0.1', 27017, 'weibo_content', 'user_content')
    with ThreadPoolExecutor(max_workers=process_num) as executor:
        future_to_proc = {executor.submit(extractor.content2mongo, join(msg_dir, file), target_list): file for file in jobs}
        for future in futures.as_completed(future_to_proc):
            logger.info(u'Finish processing %s'%future_to_proc[future])
            if future.exception() is not None:
                logger.info(future.exception()) 

if __name__ == '__main__':
    if len(sys.argv)<4:
        print "Please input msg directory, process number and target_list file"
        sys.exit(0)
    run(sys.argv[1], int(sys.argv[2]), sys.argv[3])