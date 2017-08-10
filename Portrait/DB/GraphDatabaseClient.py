# -*- coding:utf8 -*-

__author__="King.W"

import sys

sys.path.append("../")

import json
from py2neo import Graph, Node, Relationship #py2neo 3.0
from utils.GetConfig import GetConfig
from utils.utilFunction import fuzzy_condition_clause_format, acc_condition_clause_format
from utils.LogHandler import LogHandler

class GraphDatabaseClient(object):

	def setDatabase(self, db_host, db_port, db_user, db_password):
		self.db_host = db_host
		self.db_port = db_port
		self.db_user = db_user
		self.db_password = db_password
		self.graph = Graph(host=self.db_host, bolt=True, bolt_port=self.db_port, user="neo4j", password="ictsoftware")
		self.log = LogHandler("graphdb_client")

	def insert_or_update_node(self, label, key, vals):
		"""
		Insert if the node is not in the dataset or update the existed relationship. 

		Parameters:
		label: label; 
		key: 		key; 
		properties: json string.

		Example: insert_or_update_node("Test", "yexuliang", {"Test":"123123", "has_crawled" : 1})
		"""
		nd = Node(label, neo_id="neo_"+str(key))
		self.graph.merge(nd)
		for k,v in vals.iteritems():
			nd[k] = v
		self.graph.push(nd)

	def insert_or_update_relation(self, rel_type, st_nd, end_nd, vals):
		"""
		Insert if the relationship is not in the dataset or update the existed relationship. 

		Parameters:
		rel_type: type of relationship; 
		st_nd: start node; 
		end_nd: end node; 
		bidirectional: True or False; 
		vals: json string.

		Example: insert_or_update_relation("T_Rel", Node("Test", uid="yexuliang"), Node("Test", uid="3310858"), {"clue":["id", "screen_name"]})
		"""
		rel = Relationship(st_nd, rel_type, end_nd)
		self.graph.merge(rel)
		for k,v in vals.iteritems():
			rel[k] = v
		self.graph.push(rel)

	def find_node_by_id(self, label, key):
		"""
		Find nodes by neo_id. 

		Parameters:
		label: label;
		key: neo_id.

		Example: find_node_by_id("Douban", "neo_3513921")
		"""
		nds = self.graph.data("MATCH (nd:{} {{neo_id: '{}'}}) RETURN nd".format(label,key))

		if nds:
			return nds
		else:
			return None

	def find_node_by_fuzzy_property(self, label, nd_info, limit=None):
		"""
		Find node by fuzzy conditions. 

		Parameters:
		label: label; 
		nd_info: dictionary where key is the property_name and value is the property_val;
		limit: default None. limited number of query results.

		Example: find_node_by_fuzzy_property("Douban", {"name":"ta"}, 1)
		"""
		cql = "MATCH (nd:{})".format(label)
		condition_clause = fuzzy_condition_clause_format('nd', nd_info)
		if condition_clause:
			cql += " WHERE "+condition_clause+" RETURN nd"
		else:
			cql += " RETURN nd"

		if limit:
			cql += " LIMIT {}".format(limit)

		self.log.info(u"Query: %s"%cql)

		nds = self.graph.data(cql)

		if nds:
			return nds
		else:
			return None

	def find_node_by_acc_property(self, label, nd_info, limit=None):
		"""
		Find node by accurate conditions. 

		Parameters:
		label: label; 
		nd_info: dictionary where key is the property_name and value is the property_val;
		limit: default None. limited number of query results.
		
		Example: find_rel_by_acc_property('id', {'label':'Douban', 'name':'tada'},\
			 {'label':'Weibo', 'nick_name':'tadamiracle'})
		"""
		cql = "MATCH (nd:{})".format(label)
		condition_clause = acc_condition_clause_format('nd', nd_info)
		if condition_clause:
			cql += " WHERE "+condition_clause+" RETURN nd"
		else:
			cql += " RETURN nd"

		if limit:
			cql += " LIMIT {}".format(limit)

		self.log.info(u"Query: %s"%cql)

		nds = self.graph.data(cql)

		if nds:
			return nds
		else:
			return None

	def find_rel_by_fuzzy_property(self, rel_type, st_nd, end_nd, limit=None):
		"""
		Find relationship by fuzzy conditions.

		Parameters:
		rel_type: relation type; 
		st_nd: start node info in dictionary where key is the property_name and value is the property_val.
		end_nd: end node info in dictionary where key is the property_name and value is the property_val.
		limit: default None. limited number of query results.

		Example: find_rel_by_fuzzy_property('id', {'label':'Douban', 'name':'ta'}, {'label':'Weibo'}, 2)
		"""
		if 'label' in st_nd and 'label' in end_nd:
			cql = "MATCH (st_nd:{})-[a:ALIGN]->(end_nd:{})".format(st_nd['label'], end_nd['label'])
		else:
			self.log.warning(u"No specific relation type in 'find_rel_by_fuzzy_property'")
			return None

		st_cond_clause = fuzzy_condition_clause_format('st_nd', st_nd)
		end_cond_clause = fuzzy_condition_clause_format('end_nd', end_nd)
		condition_clause = st_cond_clause
		if end_cond_clause:
			condition_clause += " AND "+end_cond_clause
		if rel_type:
			condition_clause += " AND '{}' in a.align_msg".format(rel_type)

		if condition_clause:
			cql += " WHERE "+condition_clause+" RETURN st_nd, a, end_nd"
		else:
			cql += " RETURN st_nd, a, end_nd"

		if limit:
			cql += " LIMIT {}".format(limit)

		self.log.info(u"Query: %s"%cql)

		rels = self.graph.data(cql)

		if rels:
			return rels
		else:
			return None

	def find_rel_by_acc_property(self, rel_type, st_nd, end_nd, limit=None):
		"""
		Find relationship by accurate conditions.

		Parameters:
		rel_type: relation type; 
		st_nd: start node info in dictionary where key is the property_name and value is the property_val.
		end_nd: end node info in dictionary where key is the property_name and value is the property_val.
		limit: default None. limited number of query results.

		Example: find_rel_by_fuzzy_property('id', {'label':'Douban', 'name':'ta'}, {'label':'Weibo'}, 2)
		"""
		if 'label' in st_nd and 'label' in end_nd:
			cql = "MATCH (st_nd:{})-[a:ALIGN]->(end_nd:{})".format(st_nd['label'], end_nd['label'])
		else:
			self.log.warning(u"No specific relation type in 'find_rel_by_acc_property'")
			return None

		st_cond_clause = acc_condition_clause_format('st_nd', st_nd)
		end_cond_clause = acc_condition_clause_format('end_nd', end_nd)
		condition_clause = st_cond_clause
		if end_cond_clause:
			condition_clause += " AND "+end_cond_clause
		if rel_type:
			condition_clause += " AND '{}' in a.align_msg".format(rel_type)

		cql += " WHERE "+condition_clause+" RETURN st_nd, a, end_nd"

		if limit:
			cql += " LIMIT {}".format(limit)

		self.log.info(u"Query: %s"%cql)

		rels = self.graph.data(cql)

		if rels:
			return rels
		else:
			return None

	def clear(self):
		self.graph.delete_all()

if __name__ == '__main__':
	db = GraphDatabaseClient()
	config = GetConfig()
	db.setDatabase(config.host, config.port, config.user, config.password)
	# db.setDatabase()
	# db.insert_or_update_node("Test", "yexuliang", {"Test":"123123", "has_crawled" : 1, "has_got_rels" : 1, "uid" : "yexuliang", "icon_avatar" : "https://img3.doubanio.com/view/site/icon/public/cbbdcb573b48b0e.jpg", "statuses_count" : 0, "name" : "聆听诗歌", "following_count" : 0, "created" : "", "type" : "site", "large_avatar" : "https://img3.doubanio.com/view/site/median/public/cbbdcb573b48b0e.jpg", "followers_count" : 150, "albums_count" : 0, "avatar" : "https://img3.doubanio.com/view/site/small/public/cbbdcb573b48b0e.jpg", "is_follower" : False, "signature" : None, "following" : False, "alt" : "https://site.douban.com/yexuliang/", "desc" : "", "notes_count" : 0, "id" : "212230" })
	# db.insert_or_update_relation("T_Rel", Node("Test", uid="yexuliang"), Node("Test", uid="3310858"), {"clue":["id", "screen_name"]})
	print db.find_node_by_id("Douban", "neo_3513921")
	print db.find_node_by_fuzzy_property("Douban", {"name":"ta"}, 1)
	print db.find_node_by_acc_property("Douban", {"name":"tada"}, 1)
	print db.find_rel_by_acc_property('id', {'label':'Douban', 'name':'tada'},\
			 {'label':'Weibo', 'nick_name':'tadamiracle'})
	print db.find_rel_by_fuzzy_property('id', {'label':'Douban', 'name':'ta'},\
			 {'label':'Weibo'}, 2)

