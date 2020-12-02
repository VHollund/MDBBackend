from app.Main import app, update_data
from apscheduler.schedulers.background import BackgroundScheduler

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_data, trigger="interval", seconds=86400)
    scheduler.start()
    app.run(debug=True, port=5002)