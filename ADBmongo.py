
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
        genre = []
        pcountry = []
        pcompany = []
        for z in x["genres"]:
            genre.append(z["name"])
        for z in x["production_countries"]:
            pcountry.append(z["iso_3166_1"])
        for z in x["production_companies"]:
            pcompany.append(z["name"])
        for g in genre:
            for pct in pcountry:
                for pcp in pcompany:
                    mov.append({
                        "id": x["id"],
                        "DimGenre": g,
                        "DimCountry": pct,
                        "DimCompany": pcp,
                        "budget": int(x["budget"]),
                        "revenue": int(x["revenue"]),
                        "vote_average": float(x["vote_average"]),
                        "vote_count": int(x["vote_count"])
                    })
    adb.insert_many(mov)

def update_ADB(movies):
    for x in movies.find():
        genre = []
        pcountry = []
        pcompany = []
        for z in x["genres"]:
            genre.append(z["name"])
        for z in x["production_countries"]:
            pcountry.append(z["iso_3166_1"])
        for z in x["production_companies"]:
            for g in genre:
                for pct in pcountry:
                    for pcp in pcompany:
                        adb.update({"id": x["id"],
                                    "DimGenre":g,
                                    "DimCountry":pct,
                                    "DimCompany":pcp
                                    }, {"$set": {
                                        "budget": x["budget"],
                                        "revenue": x["revenue"],
                                        "vote_average": x["vote_average"],
                                        "vote_count": x["vote_count"]
                        }})
