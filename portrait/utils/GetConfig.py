# -*- coding:utf8 -*-

__author__='King.W'

import sys

sys.path.append("../../")

import os
from portrait.utils.utilClass import ConfigParse
from portrait.utils.utilClass import LazyProperty

class GetConfig(object):

	def __init__(self):
		self.pwd = os.path.split(os.path.realpath(__file__))[0] # current directory
		self.config_path = os.path.join(os.path.split(self.pwd)[0], 'config.ini') # the upper level of current directory
		self.config_file = ConfigParse()
		self.config_file.read(self.config_path)

	# graphdb configuration
	@LazyProperty
	def graphdb_host(self):
		return self.config_file.get('GraphDB','host')

	@LazyProperty
	def graphdb_port(self):
		return int(self.config_file.get('GraphDB', 'port'))

	@LazyProperty
	def graphdb_user(self):
		return self.config_file.get('GraphDB', 'user')

	@LazyProperty
	def graphdb_password(self):
		return self.config_file.get('GraphDB', 'password')
	# end of graphdb configuration

	# doubandb configuration
	@LazyProperty
	def doubandb_host(self):
		return self.config_file.get('DoubanDB','host')

	@LazyProperty
	def doubandb_port(self):
		return int(self.config_file.get('DoubanDB', 'port'))

	@LazyProperty
	def doubandb_name(self):
		return self.config_file.get('DoubanDB', 'name')

	@LazyProperty
	def doubandb_tab(self):
		return self.config_file.get('DoubanDB', 'tab')
	# end of doubandb configuration

	# weibodb configuration
	@LazyProperty
	def weibodb_host(self):
		return self.config_file.get('WeiboDB','host')

	@LazyProperty
	def weibodb_port(self):
		return int(self.config_file.get('WeiboDB', 'port'))

	@LazyProperty
	def weibodb_name(self):
		return self.config_file.get('WeiboDB', 'name')

	@LazyProperty
	def weibodb_tab(self):
		return self.config_file.get('WeiboDB', 'tab')
	# end of weibodb configuration	

	# relationdb configuration
	@LazyProperty
	def reldb_host(self):
		return self.config_file.get('RelationDB','host')

	@LazyProperty
	def reldb_port(self):
		return int(self.config_file.get('RelationDB', 'port'))

	@LazyProperty
	def reldb_name(self):
		return self.config_file.get('RelationDB', 'name')

	@LazyProperty
	def reldb_tab(self):
		return self.config_file.get('RelationDB', 'tab')
	# end of relationdb configuration	

	# namehash configuration
	@LazyProperty
	def namehashdb_host(self):
		return self.config_file.get('NameHashDB','host')

	@LazyProperty
	def namehashdb_port(self):
		return int(self.config_file.get('NameHashDB', 'port'))

	@LazyProperty
	def namehashdb_name(self):
		return self.config_file.get('NameHashDB', 'name')
	# end of relationdb configuration	

	# nethash configuration
	@LazyProperty
	def nethashdb_host(self):
		return self.config_file.get('NetHashDB','host')

	@LazyProperty
	def nethashdb_port(self):
		return int(self.config_file.get('NetHashDB', 'port'))

	@LazyProperty
	def nethashdb_name(self):
		return self.config_file.get('NetHashDB', 'name')
	# end of relationdb configuration	