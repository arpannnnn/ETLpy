import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# DB Credentials from .env
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Create SQLAlchemy engine
engine = create_engine(db_url)

# Read CSV
df = pd.read_csv('users.csv')
df['signup_date'] = pd.to_datetime(df['signup_date'])

# -- Load dim_users
dim_users = df[['name', 'email', 'age', 'gender']].drop_duplicates()
dim_users.to_sql('dim_users', engine, if_exists='append', index=False)

# -- Load dim_date
dim_date = df[['signup_date']].drop_duplicates()
dim_date['year'] = dim_date['signup_date'].dt.year
dim_date['month'] = dim_date['signup_date'].dt.month
dim_date['day'] = dim_date['signup_date'].dt.day
dim_date.rename(columns={'signup_date': 'full_date'}, inplace=True)
dim_date.to_sql('dim_date', engine, if_exists='append', index=False)

# -- Load fact_signups
# Join back with IDs
dim_users_db = pd.read_sql("SELECT * FROM dim_users", engine)
dim_date_db = pd.read_sql("SELECT * FROM dim_date", engine)

merged = df.merge(dim_users_db, on=['name', 'email', 'age', 'gender'])
merged = merged.merge(dim_date_db, left_on='signup_date', right_on='full_date')
fact = merged[['user_id', 'date_id', 'signup_channel']]
fact.to_sql('fact_signups', engine, if_exists='append', index=False)

print("✅ ETL Done: Data loaded into warehouse.")
