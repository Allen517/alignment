# -*- coding:utf-8 -*-

__author__ = 'Maps'

import sys,os

sys.path.append("../../")

from pymongo import MongoClient
from portrait.utils.utilClass import Singleton

class MongodbClient(object):

	def setDatabase(self, db_host, db_port, db_name, tab_name):
		self.client = MongoClient(db_host, db_port)
		self.db_name = db_name
		self.db = self.client[db_name]
		self.tab = tab_name

	def changeTable(self, name):
		self.tab_name = name

	def put(self, key, dbComm):
		if self.db[self.tab].find_one(key):
			return None
		else:
			self.db[self.tab].insert(dbComm)

	def update(self, key, val):
		if not self.db[self.tab].find_one(key):
			self.put(key, val)
		else:
			self.db[self.tab].update(key,{"$set":val})

	def delete(self, key):
		self.db[self.tab].remove(key)

	def search(self, prop_name, prop_vals, search_opera):
		for p in self.db[self.tab].find({prop_name:{search_opera:prop_vals}}):
			yield p

	def get(self, key):
		if not self.db[self.tab].find_one(key):
			return None
		else:
			return self.db[self.tab].find_one(key)

	def getAll(self, key=None):
		for p in self.db[self.tab].find({}):
			yield p

	def clean(self):
		self.client.drop_database(self.db_name)

	def deleteAll(self):
		self.db[self.tab].remove()

if __name__ == "__main__":
	db.clean()

