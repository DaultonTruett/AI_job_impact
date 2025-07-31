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

    

