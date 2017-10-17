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

logger = LogHandler(__name__)
nick2uid_map = dict()
mutex = Lock()

class WeiboNetExtractor(object):

    def __init__(self, host, port, db_name, db_user_collect, db_net_collect):
        self.db_user = self.__loadMongodb(host, port, db_name, db_user_collect)
        self.db_net = self.__loadMongodb(host, port, db_name, db_net_collect)
        except_file = 'net_extract_exception.dat.'+time.strftime('%Y-%m-%d',time.localtime(time.time()))
        self.except_handler = open(except_file, 'w')
        nomatched_users_file = 'no_match_users.dat.'+time.strftime('%Y-%m-%d',time.localtime(time.time()))
        self.nomatched_users_handler = open(nomatched_users_file, 'w')

    def __loadMongodb(self, host, port, db_name, db_collect):
        db = MongodbClient()
        db.setDatabase(host, port, db_name, db_collect)
        return db

    def __getRegStr(self, regex, str):
        match_res = re.findall(regex, str)
        return match_res

    def __updateDict(self, d_set, key_list):
        for key in key_list:
            if key:
                if key in d_set:
                    d_set[key] += 1
                else:
                    d_set[key] = 1
        return d_set

    def __idExtractor(self, nick2uid_map, info, judge_cond, regex, msg_type):
        if 'msg_type' in info and 'msg_text' in info and info['msg_type']==msg_type:
            if judge_cond in info['msg_text']:
                match_res = self.__getRegStr(regex, info['msg_text'])
                if match_res:
                    for nick in match_res[0]:
                        if nick:
                            if nick in nick2uid_map:
                                uid = nick2uid_map[nick]
                                yield uid
                            else:
                                self.__write2nomatched_users(nick.encode('utf8')+'\n')
                                yield nick
            elif 'root_uid' in info:
                yield info['root_uid']

    def __nickExtractor(self, info, judge_cond, regex, msg_type):
        if 'msg_type' in info and 'msg_text' in info and info['msg_type']==msg_type:
            if judge_cond in info['msg_text']:
                match_res = self.__getRegStr(regex, info['msg_text'])
                if match_res:
                    for nick in match_res[0]:
                        if nick:
                            yield nick

    def __data2mongo(self, uid, child_uid_list):
        if uid:
            u_info = self.db_net.get({'uid':uid})
            if not u_info:
                u_info = {}
            u_info['uid'] = uid
            u_info = self.__updateDict(u_info, child_uid_list)
            self.db_net.update({'uid':uid},u_info)

    def __write2exception_file(self, str):
        self.except_handler.write(str)

    def __write2nomatched_users(self, str):
        self.nomatched_users_handler.write(str)

    def __build_nick2uid_map(self, nick2uid_map, nick_batch_set):
        if nick_batch_set is None:
            return None
        search_list = list()
        for nick in nick_batch_set:
            if nick not in nick2uid_map:
                search_list.append(nick)
        if not search_list:
            return None
        uid_search_res = self.db_user.search('nick_name', list(search_list), '$in')
        local_nick2uid_map = {}
        for res in uid_search_res:
            if 'nick_name' in res and 'uid' in res:
                local_nick2uid_map[res['nick_name']]=res['uid']
        return local_nick2uid_map

    def __proc_set_records(self, process_batch_set, nick_batch_set):
        global nick2uid_map
        local_nick2uid_map = self.__build_nick2uid_map(nick2uid_map, nick_batch_set)
        if local_nick2uid_map:
            if mutex.acquire():
                nick2uid_map.update(local_nick2uid_map)
                print "Now size of nick2uid_map: %d"%len(nick2uid_map)
                mutex.release()
        for record in process_batch_set:
            # get follower id
            uid_list = list(self.__idExtractor(nick2uid_map, record, u'//@', u'//@(.*?):|//@(.*?)：', 3))
            # store in database
            if uid_list:
                self.__data2mongo(record['uid'], uid_list)
            else:
                self.__write2exception_file(json.dumps(record))

    # root_uid, mid, uid, msg_text
    def content2mongo(self, data_file, target_uid=set()):
        global logger
        logger.info("start to process %s"%data_file)
        with open(data_file, 'r') as objF:
            over_cnt = 0
            cnt = 0
            process_batch_set = tuple()
            nick_batch_set = set()
            for line in objF:
                over_cnt += 1
                cinfo = json.loads(line)
                retry_flag = False  
                if target_uid:
                    if 'uid' not in cinfo or 'msg_type' not in cinfo or cinfo['msg_type']!=3 or cinfo['uid'] not in target_uid:
                        continue
                else:
                    if 'uid' not in cinfo or 'msg_type' not in cinfo or cinfo['msg_type']!=3:
                        continue
                # fetch batch data
                cnt += 1
                process_batch_set += cinfo,
                nicks = set(self.__nickExtractor(cinfo, u'//@', u'//@(.*?):|//@(.*?)：', 3))
                if nicks:
                    nick_batch_set.update(nicks)
                # process batch data
                if cnt%1000==0:
                    self.__proc_set_records(process_batch_set, nick_batch_set)
                    logger.info(u"Processing %s: #%d(%d) records restored in mongo"%(data_file, cnt, over_cnt))
                    process_batch_set = tuple()
                    nick_batch_set = set()

            self.__proc_set_records(process_batch_set, nick_batch_set)
            logger.info(u"Processing %s: #%d(%d) records restored in mongo"%(data_file, cnt, over_cnt))
            logger.info(u"Overall %d records in %s"%(cnt, data_file))

def read_target_list(target_list_file):
    target_list = set()
    with open(target_list_file, 'r') as tlFile:
        for line in tlFile:
            target_list.add(line.strip())
    print u'load %d target uids from %s'%(len(target_list), target_list_file)
    return target_list

def run(msg_dir, process_num=5, target_list_file=''):
    target_list = set()
    if target_list_file:
        target_list = read_target_list(target_list_file)

    ignore_num = 7
    jobs = listdir(msg_dir)
    jobs = jobs[ignore_num:]
    extractor = WeiboNetExtractor('127.0.0.1', 27017, 'weibo_content', 'nick_name2uid', 'social_interact')
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