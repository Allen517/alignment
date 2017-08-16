# encoding: utf-8
'''
Created on 2017年8月7日

@author: alibaba
'''
import neo2s
from py2neo import Graph, Node, Relationship
neo_graph = Graph(
    "http://10.200.6.5:7474/db/data/", 
    username="neo4jTest", 
    password="ictsoftware"
)
from pymongo import MongoClient
##已有对应关联关系数据库
conn1=MongoClient("10.200.6.7",27017)
rel_db=conn1.douban_weibo.douban_weibo

##豆瓣信息数据库
conn2=MongoClient("10.61.2.218",27017)
douban_db=conn2.douban.users

##微博信息数据库
conn3=MongoClient("10.200.6.5",27017)
weibo_db=conn3.weibo.user_info


rels=list(rel_db.find()) ##.skip(10).limit(100)
ct=1
for rel in rels:
    neo_tx = neo_graph.begin()
    doubanId = rel["doubanId"].__str__()
    print "No." +str(ct)+"-doubanId:" + doubanId + "----------start------------"
    print rel
    doubanUsers=list(douban_db.find({"uid":doubanId}).limit(1))
    doubanUser=doubanUsers[0]
    print doubanUser
    doubanNode=neo2s.createNode(neo_graph, "Douban", "id", doubanUser)
    neo_graph.push(doubanNode)  ##加入relationship的时候会自动插入节点数据
    ##neo_tx.create(doubanNode)
    weiboIds=rel["weiboIds"]
    for weiboId in weiboIds:
        weiboUid=weiboId["uid"].__str__()
        print weiboUid
        weiboUsers=list(weibo_db.find({"uid":weiboUid}))
        print weiboUsers[0]
        weiboNode=neo2s.createNode(neo_graph, "Weibo", "uid", weiboUsers[0])
        neo_graph.push(weiboNode)
        ##neo_tx.create(weiboNode)
        d2wRelation=Relationship(doubanNode,"ALIGN",weiboNode,align_msg=["id"]) #
        neo_tx.create(d2wRelation)
        w2dRelation=Relationship(weiboNode,"ALIGN",doubanNode,align_msg=["id"])
        neo_tx.create(w2dRelation)
    neo_tx.commit()   
    print "No." +str(ct)+"-doubanId:" +doubanId + "----------end------------"
    ct+=1