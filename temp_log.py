# Imports
from datetime import datetime
from sqlalchemy import create_engine
import config
import pandas as pd
import requests

# Variables
USERNAME = config.user
PG_STRING = config.pgs
URL = config.url

# Request
response = requests.get(config.url)
df = pd.DataFrame([[datetime.now(), response.json()['main']['temp']]],
                  columns = ['date','temp'])

# Load
engine = create_engine(PG_STRING)

with engine.connect() as conn:
    df.to_sql(
        "temp_log",
        conn,
        schema=f"{USERNAME}/cbus_temps",
        index=False,
        if_exists='append')
