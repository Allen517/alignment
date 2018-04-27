# -*- coding:utf8 -*-

from portrait.DB.MongodbClient import MongodbClient

from collections import defaultdict

def init_db(host, port, db_name, col_name):
	db_handler = MongodbClient(host, port, db_name, col_name)
	return db_handler

def search_hash_code_in_bands(db, hash_in_bands, col_name, doc_id, band_size):
	# except source collection
	for i in range(1, band_size):
		db.changeTable('{}_{}'.format(col_name,i))
		rcd = db.get({'doc_id':doc_id})
		hash_in_bands = store_hash_code(rcd, hash_in_bands)

	return hash_in_bands

def store_hash_code(dat, hash_in_bands):
	hash_code = None
	if 'hash' in dat:
		hash_code = dat['hash']
	hash_in_bands.append(hash_code)

	return hash_in_bands

def find_align_rcd(douban_uid, align_rcd, weibo_db, weibo_col_name, hash_in_bands):
	# find same hash code in weibo_db
	for i in range(len(hash_in_bands)):
		hash_code = hash_in_bands[i]
		weibo_db.changeTable('{}_{}'.format(weibo_col_name,i))
		search_res = weibo_db.search('hash', [hash_code], '$in')
		for res in search_res:
			if 'doc_id' not in res:
				continue
			align_rcd.setdefault(douban_uid, defaultdict(lambda :0))
			align_rcd[douban_uid][res['doc_id']] += 1
			# print i, res['doc_id'], res['hash']
	return align_rcd

def write_align_rcd(align_rcd, target_file):
	with open(target_file, 'aw') as objF:
		wrtLns = ''
		for douban_id, aligns in align_rcd.iteritems():
			wrtLns += douban_id+':'
			for weibo_id, cnt in aligns.iteritems():
				wrtLns += '{},{};'.format(weibo_id, cnt)
			wrtLns = wrtLns[:-1]+'\n'
		objF.write(wrtLns)

def store_align_rcd(align_rcd, target_db):
	store_db = init_db(target_db, 27017, 'w2d_name', 'name_align')
	rcds = list()
	for douban_id, aligns in align_rcd.iteritems():
		one_rcd = dict()
		one_rcd['douban_uid']=douban_id
		rcd_flag = False
		for weibo_id, cnt in aligns.iteritems():
			if cnt>3:
				one_rcd['weibo_uid']=weibo_id
				one_rcd['hit_ct']=cnt
				rcd_flag = True
		if rcd_flag:
			rcds.append(one_rcd)
	if rcds:
		store_db.put_many(rcds)

def run():
	band_size = 6
	target_file = 'weibo_douban_name_align'
	# iteration & find the records with same hash code
	douban_col_name = 'douban_b0'
	douban_db = init_db('10.60.1.73', 27017, 'name_hash', douban_col_name)
	weibo_col_name = 'weibo_b0'
	weibo_db = init_db('10.60.1.74', 27017, 'name_hash_tmp', weibo_col_name)
	# source:douban_0
	douban_db.changeTable('{}_{}'.format(douban_col_name,0))
	# read source collection
	skip = 0
	limit = 100
	align_rcd = dict()
	source_dat = list(douban_db.getAll(limit, skip))
	# print source_dat
	while source_dat:
		print 'Reading %d records from %s.%s'%(len(source_dat), douban_db.__db_name__, douban_db.__collect_name__)
		for dat in source_dat:
			if 'doc_id' not in dat:
				continue
			doc_id = dat['doc_id']
			# store source hash_code and other hash_code in all bands
			hash_in_bands = list()
			hash_in_bands = store_hash_code(dat, hash_in_bands)
			hash_in_bands = search_hash_code_in_bands(douban_db, hash_in_bands, douban_col_name, doc_id, band_size)
			# print hash_in_bands
			# find same hash_code in weibo
			align_rcd = find_align_rcd(doc_id, align_rcd, weibo_db, weibo_col_name, hash_in_bands)
		# write_align_rcd(align_rcd, target_file)
		store_align_rcd(align_rcd, '10.61.1.245')
		align_rcd = dict()
		skip += limit
		print "Writing %d alignments in file"%(skip)
		douban_db.changeTable('{}_{}'.format(douban_col_name,0))
		# break
		source_dat = list(douban_db.getAll(limit, skip))

if __name__=='__main__':
	run()