
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
    mov = []
    for x in movies.find():
        g = []
        pcountry = []
        pcompany = []
        for z in x["genres"]:
            g.append(z["name"])
        for z in x["production_countries"]:
            pcountry.append(z["iso_3166_1"])
        for z in x["production_companies"]:
            pcompany.append(z["name"])
        mov.append({
            "id": x["id"],
            "DimGenre": g,
            "DimCountry": pcountry,
            "DimCompany": pcompany,
            "budget": x["budget"],
            "revenue": x["revenue"],
            "vote_average": x["vote_average"],
            "vote_count": x["vote_count"]
        })
    adb.insert_many(mov)

