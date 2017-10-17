# -*- coding:utf-8 -*-

__author__ = 'Maps'

import sys,os

sys.path.append("../../")

from pymongo import MongoClient
from portrait.utils.utilClass import Singleton
from portrait.utils.utilClass import LazyProperty

class MongodbClient(object):

	@LazyProperty
	def __db_host__(self):
		return self.db_host

	@LazyProperty
	def __db_port__(self):
		return self.db_port

	@LazyProperty
	def __db_name__(self):
		return self.db_name

	@LazyProperty
	def __collect_name__(self):
		return self.tab

	def setDatabase(self, db_host, db_port, db_name, tab_name):
		self.client = MongoClient(db_host, db_port)
		self.db_host = db_host
		self.db_port = db_port
		self.db_name = db_name
		self.db = self.client[db_name]
		self.tab = tab_name

	def changeTable(self, name):
		self.tab = name

	def put_many(self, records):
		if not isinstance(records, list):
			return
		self.db[self.tab].insert_many(records)

	def put(self, key, dbComm):
		if not isinstance(dbComm, dict):
			return None
		if self.db[self.tab].find_one(key):
			return None
		else:
			self.db[self.tab].insert(dbComm)

	def update(self, key, val, multi=False):
		if not isinstance(val, dict):
			return
		if not self.db[self.tab].find_one(key):
			self.put(key, val)
		else:
			self.db[self.tab].update(key,{"$set":val}, multi=multi)

	def delete(self, key):
		self.db[self.tab].remove(key)

	def search(self, prop_name, prop_vals, search_opera):
		"""
		For example: search('uid', weibo_uids, '$in') # get user infos from weibodb
		"""
		for p in self.db[self.tab].find({prop_name:{search_opera:prop_vals}}):
			yield p

	def get(self, key):
		if not self.db[self.tab].find_one(key):
			return None
		else:
			return self.db[self.tab].find_one(key)

	def getAll(self, limit=None, skip=None):
		if not limit and not skip:
			for p in self.db[self.tab].find():
				yield p
		if not skip and limit:
			for p in self.db[self.tab].find().limit(limit):
				yield p
		for p in self.db[self.tab].find().limit(limit).skip(skip):
				yield p

	def clean(self):
		self.client.drop_database(self.db_name)

	def deleteAll(self):
		self.db[self.tab].remove()

if __name__ == "__main__":
	db.clean()

