# -*- coding:utf8 -*-

from portrait.DB.MongodbClient import MongodbClient
from portrait.gadget.LSHCache import LSHCache
from portrait.utils.GetConfig import GetConfig

from pymongo import ASCENDING, DESCENDING
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures

class NameAlign(object):

	def __init__(self):
		self.config = GetConfig()
		self.db_douban = MongodbClient()
		self.db_douban.setDatabase(self.config.doubandb_host, self.config.doubandb_port
						, self.config.doubandb_name, self.config.doubandb_tab)
		self.db_weibo = MongodbClient()
		self.db_weibo.setDatabase(self.config.weibodb_host, self.config.weibodb_port
						, self.config.weibodb_name, self.config.weibodb_tab)
		self.db_lsh_douban = MongodbClient()
		self.db_lsh_douban.setDatabase(self.config.namehashdb_host, self.config.namehashdb_port
							, self.config.namehashdb_name, 'douban')
		self.db_lsh_weibo = MongodbClient()
		self.db_lsh_weibo.setDatabase(self.config.namehashdb_host, self.config.namehashdb_port
							, self.config.namehashdb_name, 'weibo')
		self._b = 20	# num of bands
		self._r = 5		# num of rows
		self._n = 100	# num of hash function: _b*_r=_n
		self.lsh_cache_douban = LSHCache(self.db_lsh_douban, self._n, self._b, self._r)
		self.lsh_cache_weibo = LSHCache(self.db_lsh_weibo, self._n, self._b, self._r)

	def _read_batch_records(self, db, limit, skip):
		return db.getAll(limit, skip)

	def _create_index(self, db):
		db_host = db.__db_host__
		db_port = db.__db_port__
		db_name = db.__db_name__
		base_col_name = db.__collect_name__
		client = MongoClient(db_host, db_port)
		for i in range(self._b):
			col_name = '{}_{}'.format(base_col_name,i)
			db = client[db_name][col_name]
			print db,db_name,col_name
			db.create_index([('doc_id', DESCENDING)])
			db.create_index([('hash', DESCENDING)])

	def hashing_douban_name_in_db(self, limit):
		skip = 0
		db_douban_records = list(self._read_batch_records(self.db_douban, limit, skip))
		while(db_douban_records):
			self.lsh_cache_douban.store_in_mongo(self.db_lsh_douban.__collect_name__, db_douban_records, 'uid', 'name')
			skip += limit
			print 'Already hashing %d records'%skip
			db_douban_records = list(self._read_batch_records(self.db_douban, limit, skip))
		self._create_index(self.db_lsh_douban)

	def hashing_weibo_name_in_db(self, limit):
		skip = 0
		db_weibo_records = list(self._read_batch_records(self.db_weibo, limit, skip))
		while(db_weibo_records):
			self.lsh_cache_douban.store_in_mongo(self.db_lsh_weibo.__collect_name__, db_weibo_records, 'uid', 'nick_name')
			skip += limit
			print 'Already hashing %d records'%skip
			db_weibo_records = list(self._read_batch_records(self.db_weibo, limit, skip))
		self._create_index(self.db_lsh_weibo)

	def hashing_test_in_db(self, limit):
		skip = 0
		db_weibo_records = [{"has_crawled" : 1, "has_got_rels" : 1, "uid" : "mshandy", "name" : "tada", "created" : "2011-09-08 20:30:18", "is_suicide" : False, "large_avatar" : "https://img3.doubanio.com/icon/up54306851-54.jpg", "avatar" : "https://img3.doubanio.com/icon/u54306851-54.jpg", "signature" : "bengbeng", "id" : "54306851", "is_banned" : False, "desc" : "\n\n\n\n", "type" : "user", "alt" : "https://www.douban.com/people/mshandy/" },
		{"has_crawled" : 1, "has_got_rels" : 1, "uid" : "doubanshuoshuo", "loc_id" : "108288", "name" : u"砚均", "created" : "2008-12-10 12:44:39", "id" : "3322901", "is_suicide" : False, "large_avatar" : "https://img3.doubanio.com/icon/up3322901-41.jpg", "avatar" : "https://img3.doubanio.com/icon/u3322901-41.jpg", "signature" : "", "loc_name" : "北京", "is_banned" : False, "desc" : "向寂静的土地说：我流。\n向急速的流水说：我在。", "type" : "user", "alt" : "https://www.douban.com/people/doubanshuoshuo/" }]
		cnt = 0
		while(db_weibo_records):
			self.lsh_cache_douban.store_in_mongo(self.db_lsh_weibo.__collect_name__, db_weibo_records, 'uid', 'name')
			skip += limit
			print 'Already hashing %d records'%skip
			db_weibo_records = [{"has_crawled" : 1, "has_got_rels" : 1, "uid" : "tadamiracle", "name" : "tada", "created" : "2009-01-18 19:02:59", "is_suicide" : False, "large_avatar" : "https://img1.doubanio.com/icon/up3513921-8.jpg", "avatar" : "https://img1.doubanio.com/icon/u3513921-8.jpg", "signature" : "没有翅膀，但俯视阳光", "id" : "3513921", "is_banned" : False, "desc" : "~~blog地址：blog.sina.com.cn/tadamiracle\n~weibo地址：weibo.com/tadamiracle\n\n所有的所有都终将会过去，变得不值一提\n\nlet it go\n不负于心\n用成长心态去面对应做的事情\n触摸事物之间的关联让人心潮澎湃\n在飘泊起伏中寻找自己的真理　只是　没有答案    \n推迟满足感\n存鹅鹅鹅鹅鹅~~~", "type" : "user", "alt" : "https://www.douban.com/people/tadamiracle/" }]
			cnt += 1
			if cnt>1:
				break
		# self._create_index(self.db_lsh_weibo)

# def run():
# 	with ThreadPoolExecutor(max_workers=2) as executor:
# 		future_to_proc = {executor.submit():'douban',
# 							executor.submit():'weibo'}
#         for future in futures.as_completed(future_to_proc):
#             logger.info(u'Finish processing %s'%future_to_proc[future])
#             if future.exception() is not None:
#                 logger.info(future.exception()) 

if __name__=='__main__':
	na = NameAlign()
	# na.hashing_test_in_db(2)
	na.hashing_douban_name_in_db(1000)
	# na.hashing_weibo_name_in_db(1000)