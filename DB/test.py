# encoding: utf-8
'''
Created on 2017年8月4日

@author: alibaba



'''

def insertWeiboInfo(neo_graph, weiboOb):
    name=weiboOb["name"]
    print "name is : "
    print name
    print len(name)
    print '--------------------'
    if (name==None) or (len(name)==0):
        print 'None'
        weiboNode = Node("WeiboInfo", name='None-id:'+str(weiboOb["id"]))
    else:
        weiboNode = Node("WeiboInfo", name=name.encode("utf-8"))
    neo_graph.create(weiboNode)
    for key in weiboOb.keys():
        print key
        keystr = key.__str__()
        print keystr
        print weiboOb[keystr]
        if keystr=="name":
            continue
        weiboValue = weiboOb[keystr]
        print weiboValue.__class__
        if type(weiboValue) is unicode:
            weiboNode[keystr] = weiboValue.encode('utf-8')
        elif isinstance(weiboValue,(int,float,bool)):
            weiboNode[keystr] = weiboValue
        else:
            weiboNode[keystr] = weiboValue.__str__()
    
    print weiboNode
    weiboNode.push()
    del weiboNode
    
from py2neo import Graph,Node,Relationship
from py2neo import watch
watch("httpstream")
neo_graph = Graph(
    "http://10.200.6.5:7474/db/data/", 
    username="neo4jTest", 
    password="ictsoftware"
)

from pymongo import MongoClient
conn=MongoClient("10.200.6.7",27017)
douban_db=conn.douban_weibo

results=list(douban_db.weibo_info.find().skip(27394).limit(1))
for weiboOb in results:
#weiboOb = results[0]
    print weiboOb["name"]
    print weiboOb["domain"]
    insertWeiboInfo(neo_graph, weiboOb)
conn.close()