# Project of Alignment

## Install

* Enter into the alignment directory

* Configure your own database server in alignment/portrait/config.ini

* Install: python setup.py install

## Usage

* Command line: data_import2neo 

> Note that, at the beginning of data import, the process will automatically create a directory log/ in your current path and record logs in log/ directory.

## Dataset

* 豆瓣数据

> 豆瓣全量用户： 10.61.2.218:27017 douban.users

> 豆瓣全量关系： 10.61.2.218:27017 douban.rels（用户的关注列表）

* 微博数据

> 微博全量用户： 10.61.2.218:27017 weibo.user_all

> 微博关联关系： 10.61.2.218:27017 weibo.user_links （start_uid是end_uid的粉丝，即start_uid关注end_uid）

> 微博2016年6月的全量消息数据： 10.60.1.73:27107 weibo.content

> 含有hashtag且存在关联关系的用户微博消息： 10.61.2.218:27017 weibo.filtered_msg

* 关联数据

> 用户关联： 10.61.1.245:27017 w2d_anchor_pred.align_result (type:label->用户标注数据, name_align->LSH用户名关联结果, net_struct->用户网络结构关联结果)

* 画像数据

> 微博画像： 10.61.2.218:27017 weibo.weibo_cal_result
