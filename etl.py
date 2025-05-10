from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# 1. Extract
df = pd.read_csv('users.csv')

# 2. Transform
df.columns = [col.lower().strip() for col in df.columns]
df['signup_date'] = pd.to_datetime(df['signup_date'])
df.dropna(inplace=True)

# 3. Load to PostgreSQL
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# Create connection engine using SQLAlchemy
db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# Load DataFrame into PostgreSQL
df.to_sql('users', engine, if_exists='replace', index=False)

# Confirm insertion
result = pd.read_sql('SELECT * FROM users', engine)
print(result)

# Scheduler
scheduler = BlockingScheduler()
scheduler.add_job(run_etl, 'cron', hour=9, minute=0)  # Runs daily at 9:00 AM

scheduler.start()
