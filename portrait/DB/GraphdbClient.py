# -*- coding:utf8 -*-

__author__="King.W"

import sys

sys.path.append("../../")

import json
from py2neo import Graph, Node, Relationship #py2neo 3.0
from portrait.utils.GetConfig import GetConfig
from portrait.utils.utilFunction import deprecated, condition_clause_format, \
		fuzzy_condition_clause_format, acc_condition_clause_format
from portrait.utils.LogHandler import LogHandler
from portrait.utils.utilClass import LazyProperty

class GraphdbClient(object):

	@LazyProperty
	def __db_host__(self):
		return self.db_host

	@LazyProperty
	def __db_port__(self):
		return self.db_port

	@LazyProperty
	def __db_user__(self):
		return self.db_user

	@LazyProperty
	def __db_password__(self):
		return self.db_password

	def get_db(self):
		return self.graph

	def __init__(self, db_host='localhost', db_port=27017, db_user='', db_password=''):
		self._setDatabase(db_host, db_port, db_user, db_password)

	def _setDatabase(self, db_host, db_port, db_user, db_password):
		self.db_host = db_host
		self.db_port = db_port
		self.db_user = db_user
		self.db_password = db_password
		self.graph = Graph(host=self.db_host, bolt=True, http=self.db_port, \
						user=self.db_user, password=self.db_password)
		self.log = LogHandler("graphdb_client", level=20)

	def insert_or_update_node(self, label, key, vals={}):
		"""
		Insert if the node is not in the dataset or update the existed relationship. 

		Parameters:
		label: label; 
		key: 		key; 
		properties: json string.

		Example: insert_or_update_node("Test", "yexuliang", {"Test":"123123", "has_crawled" : 1})
		"""
		nd = Node(label, neo_id="{}_{}".format(label.lower(),key))
		self.graph.merge(nd)
		for k,v in vals.iteritems():
			val = list()
			if isinstance(v, list):
				if isinstance(v[0], dict):
					for elem in v:
						val.append(json.dumps(elem))
			else:
				val = v
			nd[k] = val
		self.graph.push(nd)

		return nd

	def insert_or_update_relation(self, rel_type, st_nd, end_nd, vals={}):
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

		Example: find_node_by_id("Douban", "Weibo_3513921")
		"""
		nds = self.graph.data("MATCH (nd:{} {{neo_id: '{}'}}) RETURN nd".format(label,key))

		if nds:
			return nds[0]['nd']
		else:
			return None

	def find_node_by_rel(self, nd_label, nd_info, rel_type, rel_info={}, is_count=False, limit=None, skip=None):
		"""
		Find node by conditions. 

		Parameters:
		label: label; 
		nd_info: dictionary where key is the property_name and value is the property_val;
		rel_info: the filter of aligned info
		is_count: if False, return nodes; else return num of queried results
		limit: default None. limited number of query results.
		skip: for return results by segment

		Example: find_node_by_rel("Douban", {"name":"=~'ta'"}, 'ALIGN', {'ID': '>.9'})
		"""
		cql = "MATCH (nd:{})-[a:{}]-(res)".format(nd_label,rel_type)
		nd_condition_clause = condition_clause_format('nd', nd_info)
		rel_cond_clause = condition_clause_format('a', rel_info)

		condition_clause = nd_condition_clause
		if rel_cond_clause:
			condition_clause += " AND "+rel_cond_clause

		if is_count:
			ret_clause = " RETURN count(nd) as num"
		else:
			ret_clause = " RETURN res"

		if condition_clause:
			cql += " WHERE "+condition_clause+ret_clause
		else:
			cql += ret_clause

		if skip and isinstance(skip, int):
			cql += " SKIP {}".format(skip)
		if limit and isinstance(limit, int):
			cql += " LIMIT {}".format(limit)

		self.log.debug(u"Query: %s"%cql)

		nds = self.graph.run(cql)

		if is_count:
			return nds

		graph_nds = list()
		for nd in nds:
			graph_nds.append(nd['res'])

		if graph_nds:
			return graph_nds
		else:
			return None

	def find_node_by_property(self, label, nd_info=dict(), is_count=False, limit=None, skip=None):
		"""
		Find node by conditions. 

		Parameters:
		label: label; 
		nd_info: dictionary where key is the property_name and value is the property_val;
		is_count: if False, return nodes; else return num of queried results
		limit: default None. limited number of query results.
		skip: for return results by segment

		Example: find_node_by_property("Douban", {"name":"=~'ta'"}, False)
		"""
		cql = "MATCH (nd:{})".format(label)
		condition_clause = condition_clause_format('nd', nd_info)

		if is_count:
			ret_clause = " RETURN count(nd) as num"
		else:
			ret_clause = " RETURN nd"

		if condition_clause:
			cql += " WHERE "+condition_clause+ret_clause
		else:
			cql += ret_clause

		if skip and isinstance(skip, int):
			cql += " SKIP {}".format(skip)
		if limit and isinstance(limit, int):
			cql += " LIMIT {}".format(limit)

		self.log.debug(u"Query: %s"%cql)

		nds = self.graph.data(cql)

		if is_count:
			return nds

		graph_nds = list()
		for nd in nds:
			graph_nds.append(nd['nd'])

		if graph_nds:
			return graph_nds
		else:
			return None

	# @deprecated(find_node_by_property)
	# def find_node_by_fuzzy_property(self, label, nd_info, limit=None):
	# 	"""
	# 	Find node by fuzzy conditions. 

	# 	Parameters:
	# 	label: label; 
	# 	nd_info: dictionary where key is the property_name and value is the property_val;
	# 	limit: default None. limited number of query results.

	# 	Example: find_node_by_fuzzy_property("Douban", {"name":"ta"}, 1)
	# 	"""
	# 	cql = "MATCH (nd:{})".format(label)
	# 	condition_clause = fuzzy_condition_clause_format('nd', nd_info)
	# 	if condition_clause:
	# 		cql += " WHERE "+condition_clause+" RETURN nd"
	# 	else:
	# 		cql += " RETURN nd"

	# 	if limit:
	# 		cql += " LIMIT {}".format(limit)

	# 	self.log.info(u"Query: %s"%cql)

	# 	nds = self.graph.data(cql)

	# 	if nds:
	# 		return nds
	# 	else:
	# 		return None

	# @deprecated(find_node_by_property)	
	# def find_node_by_acc_property(self, label, nd_info, limit=None):
	# 	"""
	# 	Find node by accurate conditions. 

	# 	Parameters:
	# 	label: label; 
	# 	nd_info: dictionary where key is the property_name and value is the property_val;
	# 	limit: default None. limited number of query results.
		
	# 	Example: find_rel_by_acc_property('id', {'label':'Douban', 'name':'tada'},\
	# 		 {'label':'Weibo', 'nick_name':'tadamiracle'})
	# 	"""
	# 	cql = "MATCH (nd:{})".format(label)
	# 	condition_clause = acc_condition_clause_format('nd', nd_info)
	# 	if condition_clause:
	# 		cql += " WHERE "+condition_clause+" RETURN nd"
	# 	else:
	# 		cql += " RETURN nd"

	# 	if limit:
	# 		cql += " LIMIT {}".format(limit)

	# 	self.log.info(u"Query: %s"%cql)

	# 	nds = self.graph.data(cql)

	# 	if nds:
	# 		return nds
	# 	else:
	# 		return None

	def find_rel_by_property(self, st_nd, end_nd, rel_type='ALIGN', rel_info=dict(), is_count=False, limit=None, skip=None):
		"""
		Find relationship by conditions.

		Parameters:
		st_nd: start node info in dictionary where key is the property_name and value is the property_val.
		end_nd: end node info in dictionary where key is the property_name and value is the property_val.
		rel_type: the type of relationship
		rel_info: the filter of relationship info
		is_count: if False, return relationships; else return count(rel)
		limit: default None. limited number of query results
		skip: for return results by segment

		Example: find_rel_by_property({'label':'Douban', 'name':"=~'.*ta.*'"}, {'label':'Weibo'})
		"""
		if 'label' in st_nd and 'label' in end_nd:
			cql = "MATCH (st_nd:{})-[rel:{}]-(end_nd:{})".format(st_nd['label'], rel_type, end_nd['label'])
		else:
			self.log.warning(u"No specific relation type in 'find_rel_by_property'")
			return None

		st_cond_clause = condition_clause_format('st_nd', st_nd)
		end_cond_clause = condition_clause_format('end_nd', end_nd)
		rel_cond_clause = condition_clause_format('rel', rel_info)
		condition_clause = st_cond_clause
		if end_cond_clause:
			condition_clause += " AND "+end_cond_clause
		if rel_info:
			condition_clause += " AND "+rel_info

		if is_count:
			ret_clause = " RETURN count(rel) as num"
		else:
			ret_clause = " RETURN st_nd, rel, end_nd"

		if condition_clause:
			cql += " WHERE "+condition_clause+ret_clause
		else:
			cql += ret_clause

		if skip and isinstance(skip, int):
			cql += " SKIP {}".format(skip)
		if limit and isinstance(limit, int):
			cql += " LIMIT {}".format(limit)

		self.log.debug(u"Query: %s"%cql)

		rels = self.graph.data(cql)

		graph_rels = list()
		for rel in rels:
			graph_rels.append(rel['rel'])

		if graph_rels:
			return graph_rels
		else:
			return None

	# @deprecated(find_rel_by_property)
	# def find_rel_by_fuzzy_property(self, rel_type, st_nd, end_nd, rel_info, limit=None):
	# 	"""
	# 	Find relationship by fuzzy conditions.

	# 	Parameters:
	# 	rel_type: relation type; 
	# 	st_nd: start node info in dictionary where key is the property_name and value is the property_val.
	# 	end_nd: end node info in dictionary where key is the property_name and value is the property_val.
	# 	limit: default None. limited number of query results.

	# 	Example: find_rel_by_fuzzy_property('id', {'label':'Douban', 'name':'ta'}, {'label':'Weibo'}, 2)
	# 	"""
	# 	if 'label' in st_nd and 'label' in end_nd:
	# 		cql = "MATCH (st_nd:{})-[a:ALIGN]-(end_nd:{})".format(st_nd['label'], end_nd['label'])
	# 	else:
	# 		self.log.warning(u"No specific relation type in 'find_rel_by_fuzzy_property'")
	# 		return None

	# 	st_cond_clause = fuzzy_condition_clause_format('st_nd', st_nd)
	# 	end_cond_clause = fuzzy_condition_clause_format('end_nd', end_nd)
	# 	condition_clause = st_cond_clause
	# 	if end_cond_clause:
	# 		condition_clause += " AND "+end_cond_clause
	# 	if rel_type:
	# 		condition_clause += " AND '{}' in a.align_msg".format(rel_type)

	# 	if condition_clause:
	# 		cql += " WHERE "+condition_clause+" RETURN st_nd, a, end_nd"
	# 	else:
	# 		cql += " RETURN st_nd, a, end_nd"

	# 	if limit:
	# 		cql += " LIMIT {}".format(limit)

	# 	self.log.info(u"Query: %s"%cql)

	# 	rels = self.graph.data(cql)

	# 	if rels:
	# 		return rels
	# 	else:
	# 		return None

	# @deprecated(find_rel_by_property)
	# def find_rel_by_acc_property(self, rel_type, st_nd, end_nd, limit=None):
	# 	"""
	# 	Find relationship by accurate conditions.

	# 	Parameters:
	# 	rel_type: relation type; 
	# 	st_nd: start node info in dictionary where key is the property_name and value is the property_val.
	# 	end_nd: end node info in dictionary where key is the property_name and value is the property_val.
	# 	limit: default None. limited number of query results.

	# 	Example: find_rel_by_fuzzy_property('id', {'label':'Douban', 'name':'ta'}, {'label':'Weibo'}, 2)
	# 	"""
	# 	if 'label' in st_nd and 'label' in end_nd:
	# 		cql = "MATCH (st_nd:{})-[a:ALIGN]->(end_nd:{})".format(st_nd['label'], end_nd['label'])
	# 	else:
	# 		self.log.warning(u"No specific relation type in 'find_rel_by_acc_property'")
	# 		return None

	# 	st_cond_clause = acc_condition_clause_format('st_nd', st_nd)
	# 	end_cond_clause = acc_condition_clause_format('end_nd', end_nd)
	# 	condition_clause = st_cond_clause
	# 	if end_cond_clause:
	# 		condition_clause += " AND "+end_cond_clause
	# 	if rel_type:
	# 		condition_clause += " AND '{}' in a.align_msg".format(rel_type)

	# 	cql += " WHERE "+condition_clause+" RETURN st_nd, a, end_nd"

	# 	if limit:
	# 		cql += " LIMIT {}".format(limit)

	# 	self.log.info(u"Query: %s"%cql)

	# 	rels = self.graph.data(cql)

	# 	if rels:
	# 		return rels
	# 	else:
	# 		return None

	def delete(self, subgraph):
		if isinstance(subgraph, Relationship):
			self.graph.separate(subgraph)
		else:
			self.graph.delete(subgraph)

	def clear(self):
		self.graph.delete_all()

if __name__ == '__main__':
	db = GraphdbClient()
	config = GetConfig()
	db.setDatabase(config.graphdb_host, config.graphdb_port, config.graphdb_user, config.graphdb_password)
	# db.setDatabase()
	# db.insert_or_update_node("Test", "yexuliang", {"Test":"123123", "has_crawled" : 1, "has_got_rels" : 1, "uid" : "yexuliang", "icon_avatar" : "https://img3.doubanio.com/view/site/icon/public/cbbdcb573b48b0e.jpg", "statuses_count" : 0, "name" : "聆听诗歌", "following_count" : 0, "created" : "", "type" : "site", "large_avatar" : "https://img3.doubanio.com/view/site/median/public/cbbdcb573b48b0e.jpg", "followers_count" : 150, "albums_count" : 0, "avatar" : "https://img3.doubanio.com/view/site/small/public/cbbdcb573b48b0e.jpg", "is_follower" : False, "signature" : None, "following" : False, "alt" : "https://site.douban.com/yexuliang/", "desc" : "", "notes_count" : 0, "id" : "212230" })
	# db.insert_or_update_relation("T_Rel", Node("Test", uid="yexuliang"), Node("Test", uid="3310858"), {"clue":["id", "screen_name"]})
	# print db.find_node_by_id("Douban", "neo_3513921")
	print db.find_node_by_property("Douban", {"name":"=~'.*ta.*'"}, True)
	# print db.find_node_by_fuzzy_property("Douban", {"name":"ta"}, 1)
	# print db.find_node_by_acc_property("Douban", {"name":"tada"}, 1)
	# user_graph_node = db.insert_or_update_node('User', 'uuid.uuid1().get_hex()', {'test':[1,2,3], 'test2':{1,2,3}, 'test2':[{1,2,3},{2,3,4}]})
	# print user_graph_node
	# print db.find_rel_by_property({'label':'Douban', 'name':"='tada'"},\
	# 		 {'label':'Weibo', 'nick_name':"='tadamiracle'"}, {})
	print db.find_rel_by_property({'label':'Douban'},\
			 {'label':'Weibo'}, {}, True)
	# print db.find_node_by_rel('Douban', {'id':"='{}'".format('3513921')}, 'HAS')
	# print db.find_node_by_rel('Douban', {'name':"='tada11'"}, 'HAS', {})
	# print db.find_rel_by_acc_property('id', {'label':'Douban', 'name':'tada'},\
	# 		 {'label':'Weibo', 'nick_name':'tadamiracle'})
	# print db.find_rel_by_fuzzy_property('id', {'label':'Douban', 'name':'ta'},\
	# 		 {'label':'Weibo'}, 2)

