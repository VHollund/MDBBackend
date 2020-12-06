from flask import *
from flask_restful import Resource, Api
import pymongo
from bson.json_util import dumps,loads
from app.TMDBNeo4j import Neo4jConn
import requests as req
from ADBmongo import update_ADB
from flask_cors import CORS, cross_origin

app = Flask(__name__, template_folder='templates')
api = Api(app)
cors = CORS(app, origins="*")
app.config['CORS_HEADERS'] = 'Content-Type'

myclient = pymongo.MongoClient("mongodb+srv://xiado:123@cluster0.m5jhq.mongodb.net/MyDB?retryWrites=true&w=majority")


mydb = myclient["tmdb"]
movies = mydb["movies"]
credits = mydb["credits"]
adb = mydb["adb"]


def update_mongo(new_data):
    new_data = request.get()
    for x in new_data:
        movies.update({"id": x[0]}, {"$set": {
            "vote_count": f"{x[1][0]}",
            "vote_average": f"{x[0][1]}",
            "revenue": f"{x[0][2]}",
            "budget": f"{x[0][3]}"}}
        )
        print(f"MongoDB: updated movie {x[0]}")
    update_ADB(movies)


def update_neo4j(new_data):
    try:
        neo4j = Neo4jConn("bolt://v-hollund.no:7687", "neo4j", "dbiola")
        for x in new_data:
            results = neo4j.query("MATCH p=(m:Movie { movieID: toString(%i)}) "
                                  "SET m.voteAverage = toString(%i) "
                                  "SET m.voteCount=toString(%i) "
                                  "SET m.revenue=toString(%i) "
                                  "SET m.budget=toString(%i) "
                                  "return p" % (x[0], x[1][1], x[1][0], x[1][2], x[1][3]))
            print(f"Neo4j: updated movie {x[0]}")
            results = neo4j.query("Match (m:Movie)"
                                  "match (d:DimMovie {movieID:m.movieID})"
                                  "set d.rating = m.voteAverage"
                                  "set d.voteCount = m.voteCount"
                                  "Match (d)-[:is]->(factRevenue:FactRevenue)"
                                  "set factRevenue.releaseDate=m.releaseDate"
                                  "set factRevenue.revenue=m.revenue"
                                  "set factRevenue.budget=m.budget")
        neo4j.close()
    except:
        print("Unable to connect to neo4j server")


def update_data():
    id=[]
    new_data = {}
    print("Update Starting")
    for x in movies.find():
        id.append(x['id'])
    for x in id:
        res = req.get(f"https://api.themoviedb.org/3/movie/{x}?api_key=0edacbafa803b562070aa4928160edbf")
        new_data[res.json()['id']] = (res.json()['vote_count'], res.json()['vote_average'], res.json()["revenue"], res.json()["budget"])
    update_mongo(new_data)
    update_neo4j(new_data)


def update_adb():
    print("ADB update Starting")


@app.route('/')
def index():
    return "<h1>Please ignore this url, school project</h1>"


@app.route('/QueryCredits')
def get_by_actor_name():
    query=request.json
    a_list = credits.find(query)
    return dumps(a_list), 200


@app.route('/QueryMovies',methods = ['POST'])
def get_movies():
    query=request.json
    a_list = movies.find(query)
    return dumps(a_list), 200


@app.route('/QueryAdb', methods=['POST'])
def query_adb():
    query=request.json
    a_list = adb.find(query)
    return dumps(a_list), 200


@app.route('/AggregateAdb', methods=['POST'])
def aggregate_adb():
    query = request.data
    query = loads(query)
    print(query)
    a_list = adb.aggregate(query)
    return dumps(a_list), 200


@app.route('/neo4j/query',methods=['POST'])
def query_neo4j():
    query = request.json['query']
    neo4j = Neo4jConn("bolt://v-hollund.no:7687", "neo4j", "dbiola")
    results = neo4j.query(query)
    neo4j.close()
    return dumps(results), 200
