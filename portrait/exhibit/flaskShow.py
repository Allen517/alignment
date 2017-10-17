# -*- coding:utf8 -*-
#!/usr/bin/env python

import sys

sys.path.append("../../")

# from portrait.DB.GraphdbClient import GraphdbClient
from portrait.utils.GetConfig import GetConfig

from json import dumps
from flask import Flask, Response, request

from neo4jrestclient.client import GraphDatabase, Node

app = Flask(__name__, static_url_path='/static/')
gdb = GraphDatabase("http://10.200.6.5:7474", "neo4j", "ictsoftware")
# config = GetConfig()
# gdb = GraphdbClient()
# gdb.setDatabase(config.graphdb_host, config.graphdb_port, config.graphdb_user, config.graphdb_password)

@app.route("/")
def get_index():
    return app.send_static_file('index.html')


@app.route("/graph")
def get_graph():
    query = ("MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) "
             "RETURN m.title as movie, collect(a.name) as cast "
             "LIMIT {limit}")
    results = gdb.query(query,
                        params={"limit": request.args.get("limit", 100)})
    nodes = []
    rels = []
    i = 0
    for movie, cast in results:
        nodes.append({"title": movie, "label": "movie"})
        target = i
        i += 1
        for name in cast:
            actor = {"title": name, "label": "actor"}
            try:
                source = nodes.index(actor)
            except ValueError:
                nodes.append(actor)
                source = i
                i += 1
            rels.append({"source": source, "target": target})
    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")


@app.route("/search")
def get_search():
    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        gdb 
        query = ("MATCH (movie:Movie) "
                 "WHERE movie.title =~ {title} "
                 "RETURN movie")
        results = gdb.query(
            query,
            returns=Node,
            params={"title": "(?i).*" + q + ".*"}
        )
        return Response(dumps([{"movie": row.properties}
                               for [row] in results]),
                        mimetype="application/json")


@app.route("/movie/<title>")
def get_movie(title):
    query = ("MATCH (movie:Movie {title:{title}}) "
             "OPTIONAL MATCH (movie)<-[r]-(person:Person) "
             "RETURN movie.title as title,"
             "collect([person.name, "
             "         head(split(lower(type(r)), '_')), r.roles]) as cast "
             "LIMIT 1")
    results = gdb.query(query, params={"title": title})
    title, cast = results[0]
    return Response(dumps({"title": title,
                           "cast": [dict(zip(("name", "job", "role"), member))
                                    for member in cast]}),
                    mimetype="application/json")


if __name__ == '__main__':
    app.run(port=8080)
