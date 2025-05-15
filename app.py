from flask import Flask, Response, render_template, url_for
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from io import BytesIO
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

username = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

@app.route('/')
def home():
    # Render the main HTML page
    return render_template('index.html')

@app.route('/gender_pie_chart.png')
def gender_pie_chart():
    query = "SELECT * FROM students"
    df = pd.read_sql(query, con=engine)

    if 'Gender' not in df.columns:
        return "<h1>Column 'Gender' not found in data.</h1>"

    gender_counts = df['Gender'].value_counts()
    plt.figure(figsize=(6,6))
    plt.pie(
        gender_counts,
        labels=gender_counts.index,
        autopct='%1.1f%%',
        startangle=90
    )
    plt.title('Gender Distribution of Students (From PostgreSQL)')
    plt.axis('equal')

    img = BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)

    return Response(img.getvalue(), mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True)
