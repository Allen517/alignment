# -*- coding:utf8 -*-

__author__='King.W'

import sys

sys.path.append("../")

import os
from utils.utilClass import ConfigParse
from utils.utilClass import LazyProperty

class GetConfig(object):

	def __init__(self):
		self.pwd = os.path.split(os.path.realpath(__file__))[0] # current directory
		self.config_path = os.path.join(os.path.split(self.pwd)[0], 'config.ini') # the upper level of current directory
		self.config_file = ConfigParse()
		self.config_file.read(self.config_path)

	@LazyProperty
	def host(self):
		return self.config_file.get('GraphDB','host')

	@LazyProperty
	def port(self):
		return int(self.config_file.get('GraphDB', 'port'))

	@LazyProperty
	def user(self):
		return self.config_file.get('GraphDB', 'user')

	@LazyProperty
	def password(self):
		return self.config_file.get('GraphDB', 'password')