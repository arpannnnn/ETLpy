import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

username = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# Create connection string
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

# Load CSV file
csv_path = 'students.csv'  # Change this to your CSV path
df = pd.read_csv(csv_path)

print("CSV Preview:")
print(df.head())

# Upload to PostgreSQL
table_name = 'students'
df.to_sql(table_name, con=engine, if_exists='replace', index=False)
print(f"Data uploaded to table '{table_name}' in database '{database}'")

# Query data from PostgreSQL
query = f"SELECT * FROM {table_name}"
df_pg = pd.read_sql(query, con=engine)

# Visualize Gender distribution
if 'Gender' in df_pg.columns:
    gender_counts = df_pg['Gender'].value_counts()
    
    plt.pie(
        gender_counts,
        labels=gender_counts.index,
        autopct='%1.1f%%',
        startangle=90
    )
    plt.title('Gender Distribution of Students (From PostgreSQL)')
    plt.axis('equal')
    plt.show()
else:
    print("Column 'Gender' not found in the table.")
