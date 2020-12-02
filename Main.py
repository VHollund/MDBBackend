from flask import *
from flask_restful import Resource, Api
import pymongo
from bson.json_util import dumps
from json import load, loads
import requests
import atexit
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__, template_folder='templates')
api = Api(app)

myclient = pymongo.MongoClient("mongodb+srv://xiado:123@cluster0.m5jhq.mongodb.net/MyDB?retryWrites=true&w=majority")


mydb = myclient["tmdb"]
movies = mydb["movies"]
credits = mydb["credits"]


def update_votes():
    new_data = request.get()
    for x in new_data:
        movies.update({"id": x['id']}, {"$set": {"votes": f"{x.votes}"}})


def update_rating():
    new_data=request.get()
    for x in new_data:
        movies.update({"id": x['id']}, {"$set": {"votes": f"{x.votes}"}})


def update_data():
    update_votes()
    update_rating()


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



if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_data, trigger="interval", seconds=86400)
    scheduler.start()
    app.run(debug=True, port=5002)
