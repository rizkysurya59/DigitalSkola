import json
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json = '/Users/anggapradikta/Downloads/project_3_de/sql/schemas/user_address.json'
create_schema_sql = """create table user_address_2018_snapshots {};"""
zip_small_file = '/Users/anggapradikta/Downloads/project_3_de/temp/dataset-small.zip'
result_ingestion_check_sql = '/Users/anggapradikta/Downloads/project_3_de/sql/queries/result_ingestion_user_address.sql'
small_file_name = 'dataset-small.csv'
database='shipping_orders'
user=''
password=''
host='127.0.0.1'
port='5432'
table_name = 'user_address_2018_snapshots'



with open(schema_json, 'r') as schema:
    content = json.loads(schema.read())

list_schema = []
for c in content:
     col_name = c['column_name']
     col_type = c['column_type']
     constraint = c['is_null_able']
     ddl_list = [col_name, col_type, constraint]
     list_schema.append(ddl_list)

list_schema_2 = []
for l in list_schema:
     s = ' '.join(l)
     list_schema_2.append(s)

create_schema_sql_final = create_schema_sql.format(tuple(list_schema_2)).replace("'", "")

#Init Posgres conn
conn = pg.connect(database=database,
                  user=user,
                  password=password,
                  host=host,
                  port=port)

conn.autocommit=True
cursor=conn.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created succesfully...")
except pg.errors.DuplicateTable:
    print("table already created...")


#Load zipped file to dataframe
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name), header=None)
#chunk ada dsini, performance system yang menjalan python code yg perlu dioptimalkan

col_name_df = [c['column_name'] for c in content]
df.columns = col_name_df

df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]

#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

#insert to postgres
df_filtered.to_sql(table_name, engine, if_exists='append', index=False) 
#chunk ada dsini, performance postgresql yg harus di optimalkan

print(f'Total inserted rows: {len(df_filtered)}')
print(f'Inital created_at: {df_filtered.created_at.min()}')
print(f'Last created_at: {df_filtered.created_at.max()}')

cursor.execute(open(result_ingestion_check_sql, 'r').read())
result = cursor.fetchall()
print(result)