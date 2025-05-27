import pandas as pd
from sqlalchemy import create_engine

csv_file = 'superstore.csv'

df = pd.read_csv(csv_file, encoding='latin-1')
df = df.where(pd.notnull(df), None)


engine = create_engine('postgresql://source:source@localhost:6543/source_database')
df.to_sql('superstore_kaggle', engine)
