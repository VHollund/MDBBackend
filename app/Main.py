from flask import *
from flask_restful import Resource, Api
import pymongo
from bson.json_util import dumps
from app.TMDBNeo4j import Neo4jConn
import requests as req


app = Flask(__name__, template_folder='templates')
api = Api(app)

myclient = pymongo.MongoClient("mongodb+srv://xiado:123@cluster0.m5jhq.mongodb.net/MyDB?retryWrites=true&w=majority")


mydb = myclient["tmdb"]
movies = mydb["movies"]
credits = mydb["credits"]


def update_mongo(new_data):
    new_data = request.get()
    for x in new_data:
        movies.update({"id": x[0]}, {"$set": {"vote_count": f"{x[1][0]}", "vote_average": f"{x[0][1]}"}})
        print(f"MongoDB: updated movie {x[0]}")


def update_neo4j(new_data):
    neo4j = Neo4jConn("bolt://v-hollund.no:7687", "neo4j", "dbiola")
    for x in new_data:
        results = neo4j.query("MATCH p=(m:Movie { movieID: toString(%i)}) SET m.voteAverage = toString(%i) "
                              "SET m.voteCount=toString(%i) return p" % (x[0], x[1][1], x[1][0]))
        print(f"Neo4j: updated movie {x[0]}")

def update_data(new_data):
    id=[]
    new_data = {}
    print("Update Starting")
    for x in movies.find():
        id.append(x['id'])
    for x in id:
        res = req.get(f"https://api.themoviedb.org/3/movie/{x}?api_key=0edacbafa803b562070aa4928160edbf")
        new_data[res.json()['id']] = (res.json()['vote_count'], res.json()['vote_average'])
    update_mongo(new_data)
    update_neo4j(new_data)



@app.route('/')
def index():
    return "<h1>Please ignore this url, school project</h1>"


@app.route('/QueryCredits')
def get_by_actor_name():
    query=request.json
    a_list = credits.find(query)
    return dumps(a_list), 200


@app.route('/QueryMovies')
def get_movies():
    query=request.json
    a_list = movies.find(query)
    return dumps(a_list), 200

@app.route('/neo4j/query')
def query_neo4j():
    query = request.json['query']
    neo4j = Neo4jConn("bolt://v-hollund.no:7687", "neo4j", "dbiola")
    results = neo4j.query(query)
    neo4j.close()
    return dumps(results), 200

