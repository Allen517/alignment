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

class DataImport2Neo(object):

	def __init__(self):
		self.config = GetConfig()
		self.graphdb = GraphdbClient(self.config.graphdb_host, self.config.graphdb_port, \
									self.config.graphdb_user, self.config.graphdb_password)
		self.doubandb = MongodbClient()
		self.doubandb.setDatabase(self.config.doubandb_host, self.config.doubandb_port, \
									self.config.doubandb_name, self.config.doubandb_tab)
		self.weibodb = MongodbClient()
		self.weibodb.setDatabase(self.config.weibodb_host, self.config.weibodb_port, \
									self.config.weibodb_name, self.config.weibodb_tab)
		self.reldb = MongodbClient()
		self.reldb.setDatabase(self.config.reldb_host, self.config.reldb_port, \
									self.config.reldb_name, self.config.reldb_tab)
		self.logger = LogHandler('data_import2neo')

	def __get_rel(self):
		return self.reldb.getAll()

	def graphdb_transaction(func):
		def wrapper(self, douban_uid_set, weibo_uid_set):
			graphdb_tx = self.graphdb.graph.begin()
			func(self, douban_uid_set, weibo_uid_set)
			graphdb_tx.commit()
		return wrapper			

	def graphdb_transaction_2(func):
		def wrapper(self, douban_uid_set):
			graphdb_tx = self.graphdb.graph.begin()
			func(self, douban_uid_set)
			graphdb_tx.commit()
		return wrapper			

	@graphdb_transaction
	def __relation_data_storage(self, douban_uid_set, weibo_uid_set):
		'''
		将关系数据导入到graph db中
		'''
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
					else:
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

	def relation_data_proc(self, batch_proc_num):
		# 1. get relationships
		rels = self.__get_rel()
		# 2.1 initialization
		proc_num = 0
		douban_uid_set = tuple()
		weibo_uid_set = list()
		# 2.2 start to process relationships
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

	@graphdb_transaction_2
	def __missed_relation_data_storage(self, douban_uid_set):
		'''
		将还未导入到graph db中的豆瓣-微博数据导入
		'''
		print '#%d Missed Douban Ids'%len(douban_uid_set)
		for k in range(len(douban_uid_set)):
			douban_uid = douban_uid_set[k]
			douban_info = self.doubandb.get({'uid':douban_uid}) # get user info from doubandb
			rel_info = self.reldb.get({'doubanId':douban_uid})
			if 'weiboIds' not in rel_info:
				continue
			weibo_uids = list()
			weibo_id_info = rel_info['weiboIds']
			for id_info in weibo_id_info:
				if 'uid' in id_info:
					weibo_uids.append(id_info['uid'])
			weibo_infos = list(self.weibodb.search('uid', weibo_uids, '$in')) # get user infos from weibodb
			if '_id' in douban_info: # remove automatically generated key '_id' with type of ObjectId
				douban_info.pop('_id')
			# set and store graph node of douban
			user_graph_node = None
			if 'id' in douban_info: 
				douban_grpah_node = self.graphdb.insert_or_update_node('Douban', douban_info['id'], douban_info)
				# use existed user node or generate new user node in graphdb
				user_graph_node = self.graphdb.find_node_by_rel('Douban', {'id':"='{}'".format(\
																douban_grpah_node['id'])}, 'HAS')
				if user_graph_node:
					user_graph_node = user_graph_node[0]
				else:
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

	def missed_relation_data_proc(self, file_path):
		# 1. get missed douban ids
		missed_douban_ids = list()
		with open(file_path, 'r') as file_handler:
			for ln in file_handler:
				aligns = ln.strip().split(',')
				if len(aligns)<2:
					missed_douban_ids.append(aligns[0])
		# 2. storage
		self.__missed_relation_data_storage(missed_douban_ids)

	def redundant_user_clean(self):
		skip = 0
		limit = 1000
		weibo_nodes = self.graphdb.find_node_by_property("Weibo", limit=limit, skip=skip)
		while weibo_nodes:
			for w_nd in weibo_nodes:
				if 'neo_id' in w_nd: # list weibo nodes and get weibo neo_id
					w_id = w_nd['neo_id']
				if not w_id: # find HAS nodes
					continue
				user_nodes = self.graphdb.find_node_by_rel("Weibo", {"neo_id":"='{}'".format(w_id)}, 'HAS')
				if user_nodes:
					for u_node in user_nodes:
						if 'neo_id' not in u_node:
							continue
						# find douban node
						res_nodes = self.graphdb.find_node_by_rel("User", {"neo_id":"='{}'".format(u_node['neo_id'])}, 'HAS')
						# if there are no douban nodes
						if len(res_nodes)<2:
							d_rels = self.graphdb.find_rel_by_property({'label':'User', 'neo_id':"='{}'".format(u_node['neo_id'])}
											, {'label':'Weibo'}, rel_type='HAS')
							for d_rel in d_rels:
								self.graphdb.delete(d_rel)
								self.graphdb.delete(u_node)
			skip += limit
			weibo_nodes = self.graphdb.find_node_by_property("Weibo", limit=limit, skip=skip)

if __name__=='__main__':
	data2neo = DataImport2Neo()
	# data2neo.relation_data_proc(100)
	# data2neo.missed_relation_data_proc('/home/yqwang/Codes/python/alignment/portrait/analy/missed_douban_id.list')	
	data2neo.redundant_user_clean()