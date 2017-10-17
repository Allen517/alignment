# -*- coding:utf8 -*-

import sys

sys.path.append("../../")

from portrait.DB.GraphdbClient import GraphdbClient
from portrait.DB.MongodbClient import MongodbClient
from portrait.utils.GetConfig import GetConfig
from portrait.utils.utilFunction import unicode2utf8

import json
from portrait.utils.LogHandler import LogHandler
from py2neo import Node
import uuid

class NameAlignImport2Neo(object):

	def __init__(self):
		self.config = GetConfig()
		self.graphdb = GraphdbClient()
		self.graphdb.setDatabase(self.config.graphdb_host, self.config.graphdb_port, \
									self.config.graphdb_user, self.config.graphdb_password)
		self.doubandb = MongodbClient()
		self.doubandb.setDatabase(self.config.doubandb_host, self.config.doubandb_port, \
									self.config.doubandb_name, self.config.doubandb_tab)
		self.weibodb = MongodbClient()
		self.weibodb.setDatabase(self.config.weibodb_host, self.config.weibodb_port, \
									self.config.weibodb_name, self.config.weibodb_tab)
		self.logger = LogHandler('name_align_import2neo')

	def graphdb_transaction(func):
		def wrapper(self, douban_uid_set, weibo_uid_set):
			graphdb_tx = self.graphdb.graph.begin()
			func(self, douban_uid_set, weibo_uid_set)
			graphdb_tx.commit()
		return wrapper			

	@graphdb_transaction
	def __relation_data_storage(self, douban_uid_set, weibo_uid_set):
		if len(douban_uid_set)!=len(weibo_uid_set):
			self.logger.warning(u'The length of douban_uid_set and weib_uid_set is not equal. \
									The processed batch is skipped.')
			self.logger.warning(douban_uid_set)
			return 

		for k in range(len(douban_uid_set)):
			douban_uid = douban_uid_set[k]
			weibo_uids = weibo_uid_set[k]
			douban_info = self.doubandb.get({'uid':douban_uid}) # get user info from doubandb
			weibo_infos = self.weibodb.search('uid', weibo_uids, '$in') # get user infos from weibodb
			if '_id' in douban_info: # remove automatically generated key '_id' with type of ObjectId
				douban_info.pop('_id')
			# set and store graph node of douban
			user_graph_node = None
			if 'id' in douban_info: 
				douban_grpah_node = self.graphdb.insert_or_update_node('Douban', douban_info['id'], douban_info)
				# use existed user node or generate new user node in graphdb
				if not user_graph_node:
					user_graph_node = self.graphdb.find_node_by_rel('Douban', {'id':"='{}'".format(\
																	douban_grpah_node['id'])}, 'HAS')
					if user_graph_node:
						user_graph_node = user_graph_node[0]
				if not user_graph_node:
					user_graph_node = self.graphdb.insert_or_update_node('User', uuid.uuid1().get_hex())
				self.graphdb.insert_or_update_relation('HAS', user_graph_node, douban_grpah_node)
			# set and store graph node of weibo
			if weibo_infos:
				for weibo_info in weibo_infos:
					if 'uid' in weibo_info: 
						weibo_graph_node = self.graphdb.insert_or_update_node('Weibo', \
															weibo_info['uid'], weibo_info)
					# store relationship in neo4j
					self.graphdb.insert_or_update_relation('ALIGN', douban_grpah_node, \
															weibo_graph_node, {'ID':1.})
					# use existed user node or generate new user node in graphdb
					if not user_graph_node:
						user_graph_node = self.graphdb.find_node_by_rel('Weibo', {'uid':"='{}'".format(\
																				douban_grpah_node['uid'])}, 'HAS')
					if not user_graph_node:
						user_graph_node = self.graphdb.insert_or_update_node('User', uuid.uuid1().get_hex())
					self.graphdb.insert_or_update_relation('HAS', user_graph_node, weibo_graph_node)

	def storeDoubanName(self, file_name, batch_proc_num):
		with open(file_name, 'aw') as wrtF:
			skip_num = 0
			while(True):
				query_res = self.doubandb.getAll(batch_proc_num, skip_num)
				query_num = 0
				vals = list()
				for douban_res in query_res:
					query_num += 1
					if 'id' in douban_res:
						graph_res = self.graphdb.find_node_by_id("Douban", "douban_{}".format())
						if graph_res:
							continue
						if 'uid' in douban_res and 'name' in douban_res \
							and 'desc' in douban_res and 'loc_name' in douban_res:
							vals.append({'uid': douban_res['uid'], 'name':douban_res['name'], \
								 			'desc':douban_res['desc'], 'loc_name':douban_res['loc_name']})
				if not query_num:
					break
				for v in vals:
					wrtF.write(json.dumps(v, ensure_ascii=False).decode('utf8')+'\t')
					self.logger.info('已存储%d条豆瓣数据至本地'%skip_num+query_num)
				skip_num += batch_proc_num

	def relation_data_finder(self, batch_proc_num):
		skip_num = 0
		while(True):
			# 1. get weibo data from mongo
			weibo_query_res = self.weibodb.getAll(batch_proc_num, skip_num)
			query_num = 0
			for weibo_res in weibo_query_res:
				query_num += 1
				weibo_res_name = weibo_res['nick_name']

			if not query_num: # no results
				break

			skip_num += batch_proc_num

		# 1. get relationships
		rels = self.__get_rel()
		# 2.1 initialization
		proc_num = 0
		douban_uid_set = tuple()
		weibo_uid_set = list()
		# 2.2 start to process relationships
		rels = self.__get_rel()
		for rel in rels:
			proc_num += 1
			# 2.3 if processing the max of batch size, find user infos from mongodb
			if proc_num%batch_proc_num==0:
				self.__relation_data_storage(douban_uid_set, weibo_uid_set)
				self.logger.info(u'Already processing %d alignment records'%proc_num)
				douban_uid_set = tuple()
				weibo_uid_set = list()
			# 2.2 fetch douban_uid and weibo_uids from current relationship info
			douban_uid = ""
			weibo_uids = tuple()
			if "doubanId" in rel:
				douban_uid = rel["doubanId"]
			if "weiboIds" in rel:
				for weibo_id_info in rel["weiboIds"]:
					if "uid" in weibo_id_info:
						weibo_uids += unicode2utf8(weibo_id_info['uid']),
			douban_uid_set += unicode2utf8(douban_uid),
			weibo_uid_set.append(weibo_uids)
		self.__relation_data_storage(douban_uid_set, weibo_uid_set)
		self.logger.info(u'Done! Already processing %d alignment records'%proc_num)

if __name__=='__main__':
	data2neo = NameAlignImport2Neo()
	data2neo.storeDoubanName('douban_tmp', 10)