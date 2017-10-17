# -*- coding:utf8 -*-

import sys

sys.path.append('../../')

from portrait.DB.GraphdbClient import GraphdbClient
from portrait.utils.GetConfig import GetConfig

from py2neo import Node, Relationship
from Levenshtein import *
import matplotlib.pyplot as plt
from xpinyin import Pinyin

config = GetConfig()
gdb = GraphdbClient()
gdb.setDatabase(config.graphdb_host, config.graphdb_port, config.graphdb_user, config.graphdb_password)
p = Pinyin()

def count_func(rels, align_cnt, focus_pair_prop):
	for rel in rels:
		if 'st_nd' in rel and 'end_nd' in rel:
			douban_node = rel['st_nd']
			weibo_node = rel['end_nd']
			for k in range(len(focus_pair_prop)):
				pair_prop = focus_pair_prop[k]
				douban_prop = pair_prop[0]
				weibo_prop = pair_prop[1]
				if douban_prop in douban_node and weibo_prop in weibo_node:
					d_prop_val = douban_node[douban_prop]
					w_prop_val = weibo_node[weibo_prop]
					if d_prop_val in w_prop_val or w_prop_val in d_prop_val:
						align_cnt[k] += 1.
	return align_cnt

def edit_dist_count_func(rels, align_cnt, focus_pair_prop, dist_upperbound):
	for rel in rels:
		if 'st_nd' in rel and 'end_nd' in rel:
			douban_node = rel['st_nd']
			weibo_node = rel['end_nd']
			for k in range(len(focus_pair_prop)):
				pair_prop = focus_pair_prop[k]
				douban_prop = pair_prop[0]
				weibo_prop = pair_prop[1]
				if douban_prop in douban_node and weibo_prop in weibo_node:
					d_prop_val = douban_node[douban_prop]
					w_prop_val = weibo_node[weibo_prop]
					if distance(d_prop_val, w_prop_val)/float(max(len(d_prop_val),len(w_prop_val)))<=dist_upperbound:
						align_cnt[k] += 1.
	return align_cnt

def align_prop_stats():
	"""
	Get statistics from aligned user records, how those properties works 
	if using them as the proofs of alignment
	"""
	focus_pair_prop = [('name', 'nick_name')]
	align_cnt = [0 for i in range(len(focus_pair_prop))]
	# 1. get aligned user records
	no_rels = gdb.find_rel_by_property({'label':'Douban'}, {'label':'Weibo'}, {}, True)
	if len(no_rels)>0:
		if 'num' in no_rels[0]:
			no_rels = no_rels[0]['num']
	print no_rels
	# no_rels = 1000
	div = 1000.
	iter_num = int(no_rels/div)+1 if no_rels/div>.0 else int(no_rels/div)
	for i in range(iter_num):
		rels = gdb.find_rel_by_property({'label':'Douban'}, {'label':'Weibo'}, {}, False, int(div), int(i*div))
		# 2. check out the importance of properties if using them in alignment
		align_cnt = edit_dist_count_func(rels, align_cnt, focus_pair_prop, .5)
	print align_cnt

def edit_dist_distribution(rels, align_dist, align_ed_res, focus_pair_prop):
	for rel in rels:
		if 'st_nd' in rel and 'end_nd' in rel:
			douban_node = rel['st_nd']
			weibo_node = rel['end_nd']
			for k in range(len(focus_pair_prop)):
				pair_prop = focus_pair_prop[k]
				douban_prop = pair_prop[0]
				weibo_prop = pair_prop[1]
				if douban_prop in douban_node and weibo_prop in weibo_node:
					d_prop_val_pinyin = p.get_pinyin(douban_node[douban_prop]).replace('-','')
					w_prop_val_pinyin = p.get_pinyin(weibo_node[weibo_prop]).replace('-','')
					d_prop_val = douban_node[douban_prop].encode('utf8')
					w_prop_val = weibo_node[weibo_prop].encode('utf8')
					dist_val = distance(d_prop_val, w_prop_val)/float(max(len(d_prop_val),len(w_prop_val)))
					align_dist.append(dist_val)
					align_ed_res.append([d_prop_val, w_prop_val, dist_val \
								, distance(d_prop_val_pinyin, w_prop_val_pinyin) \
									/float(max(len(d_prop_val_pinyin), len(w_prop_val_pinyin)))])
	return align_dist, align_ed_res

def res_stats(align_dist, range_num):
	divisions = [0 for i in range(range_num)]
	for val in align_dist:
		index = int(val*range_num)
		if index==range_num:
			index -= 1
		divisions[index] += 1
	return divisions

def align_prop_draw():
	"""
	Get statistics from aligned user records, how those properties works 
	if using them as the proofs of alignment
	"""
	focus_pair_prop = [('name', 'nick_name')]
	align_cnt = [0 for i in range(len(focus_pair_prop))]
	align_dist = list()
	align_ed_res = list()
	# 1. get aligned user records
	no_rels = gdb.find_rel_by_property({'label':'Douban'}, {'label':'Weibo'}, {}, True)
	if len(no_rels)>0:
		if 'num' in no_rels[0]:
			no_rels = no_rels[0]['num']
	# print no_rels
	# no_rels = 1000align_dist
	div = 1000.
	iter_num = int(no_rels/div)+1 if no_rels/div>.0 else int(no_rels/div)
	for i in range(iter_num):
		rels = gdb.find_rel_by_property({'label':'Douban'}, {'label':'Weibo'}, {}, False, int(div), int(i*div))
		# 2. check out the importance of properties if using them in alignment
		align_dist,align_ed_res = edit_dist_distribution(rels, align_dist, align_ed_res, focus_pair_prop)
	range_num = 100
	y = res_stats(align_dist, range_num)
	x = [(i+1)/float(range_num) for i in range(range_num)]
	plt.plot(x,y)
	plt.show()

	with open("edit_dist.rec", 'w') as objF:
		for res_pair in align_ed_res:
			if len(res_pair)<4:
				continue
			objF.write(res_pair[0]+'\t'+res_pair[1]+'\t'+str(res_pair[2])+'\t'+str(res_pair[3])+'\n')

# print align_prop_stats()
align_prop_draw()