from app.Main import app, update_data
from apscheduler.schedulers.background import BackgroundScheduler
from ADBmongo import insert_into_ADB

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_data, trigger="interval", seconds=86400)
    #scheduler.add_job(func=update_data, 'cron', hour=0)
    scheduler.start()
    app.run()