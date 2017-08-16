# encoding: utf-8
'''
Created on 2017年8月7日

@author: alibaba
'''
from py2neo import Node, Relationship

def createNode(neo_graph, label, uidKey,dbOb):
    '''
    创建节点，返回节点对象。
    这里并不
    '''
    dbNode = Node(label, neo_id="neo_"+str(dbOb[uidKey]))
    neo_graph.create(dbNode)
    for key in dbOb.keys():
        keystr = key.__str__()
        dbValue = dbOb[keystr]
        if type(dbValue) is unicode:
            dbNode[keystr] = dbValue.encode('utf-8')
        elif isinstance(dbValue,(int,float,bool)):
            dbNode[keystr] = dbValue
        else:
            dbNode[keystr] = dbValue.__str__()
    ##neo_graph.push(dbNode)
    return dbNode