# Data analysis
import numpy as np
import pandas as pd

# Environment
import os
from dotenv import load_dotenv

# DB ORM that can be used with multiple DB systems, can create tables
from sqlalchemy import create_engine

# PostgreSQL specific DB driver for python, useful for raw queries
import psycopg

class DB:
    def __init__(self):
        load_dotenv();
        self.conn = None;
        self.DB_URI = os.getenv('DB_URI');
        self.dataset_path = 'raw_data/ai_job_trends_dataset.csv';


    def connect(self):
        try:            
            self.conn = psycopg.connect(conninfo=self.DB_URI);
            print('Connected to PostgreSQL.');
        
        except (Exception, psycopg.DatabaseError) as error:
            print(error);
    

    def disconnect(self):
        if self.conn is not None:
            self.conn.close();
            print("DB connection closed.");
            
            
    def createTable(self):
        # Extract data into a dataframe
        dataset = os.getenv('DATASET_PATH');
        df = pd.read_csv(dataset);
        
        # Transform the data to usable forms and best practice column naming convention
        date_cols = {
            'Date': 'Date',
            'Year': 'Year',
            'Month': 'Month',
            'Day': 'Day',
            'Time': 'Time'
        };
        
        # Depending on the dataset, duplicate values may or may not need to be removed
        # or further analysis may be required to test the validity of the duplication
        df.drop_duplicates(keep='first');
        
        # remove a record if nothing is present in any of the cols
        df.dropna(how='all');
        
        for date in date_cols.values():
            if date in df.columns:
                df[date] = pd.to_datetime(df[date]);
        
        df.columns = df.columns.str.lower();
        df.columns = df.columns.str.strip();
        df.columns = df.columns.str.replace(' ', '_');
        df.columns = df.columns.str.replace('/', '_');
        df.columns = df.columns.str.replace('(%)', 'pct');
        df.columns = df.columns.str.replace('(', '');
        df.columns = df.columns.str.replace(')', '');
        
        df.dropna(subset='automation_risk_pct');
        
        # Load the data into a new table in the DB
        try:
            engine = create_engine(self.DB_URI);
            df.to_sql(name='ai_job_trends', con=engine, index=False, if_exists='fail');
            print('New table created!')
                
        except(ValueError) as e:
            print(e)


    def readAll(self):
        cursor = self.conn.cursor()
        
        query = """SELECT * FROM ai_job_trends;""";
        cursor.execute(query);
        data = cursor.fetchall();
        
        df = pd.DataFrame.from_records(data, columns=[name[0] for name in cursor.description]);
        
        cursor.close();
        
        return df;
