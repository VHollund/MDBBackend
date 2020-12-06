
import pymongo
from bson.json_util import dumps
from app.TMDBNeo4j import Neo4jConn
import requests as req


myclient = pymongo.MongoClient("mongodb+srv://xiado:123@cluster0.m5jhq.mongodb.net/MyDB?retryWrites=true&w=majority")


mydb = myclient["tmdb"]
movies = mydb["movies"]
credits = mydb["credits"]
adb = mydb["adb"]


def insert_into_ADB():
    g = []
    pcountry = []
    pcompany = []
    mov = []
    for x in movies.find():
        g.append(x["genre"])
        pcountry.append(x["production_countries"])
        pcompany.append(x[pcompany])
        mov.append({
            "id": x["id"],
            "budget": x["budget"],
            "revenue": x["revenue"],
            "vote_average": x["vote_average"],
            "vote_count": x["vote_count"]
        })
    g = set(g)
    pcompany = set(pcompany)
    pcountry = set(pcountry)

