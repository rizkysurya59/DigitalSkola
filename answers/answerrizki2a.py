import json
from unittest import result
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json = 'sql/schemas/user_address.json'
create_schema_sql = """create table user_address_2018_snapshots {};"""
zip_small_file = 'temp/dataset-small.zip'
result_ingestion_check_sql = 'sql/queries/result_ingestion_user_address.sql'
small_file_name = 'dataset-small.csv'
database='shipping_orders'
user='postgres'
password='sword1st'
host='127.0.0.1'
port='5432'
table_name = 'user_address_2018_snapshots'

with open(schema_json, 'r') as schema:
    content = json.loads(schema.read()) 

list_schema = []
for i in content:
    col_name = i['column_name']
    col_type = i['column_type']
    constraint = i['is_null_able']
    ddl_list = [col_name,col_type,constraint]
    list_schema.append(ddl_list)

list_schema_2 = []
for x in list_schema:
    s = ' '.join(x)
    list_schema_2.append(s)

create_schema_sql_final = create_schema_sql.format(tuple(list_schema_2)).replace("'","")

#init posgres conn
conn = pg.connect(database=database, 
                  user=user,
                  password=password,
                  host=host,
                  port=port)

conn.autocommit=True
cursor=conn.cursor()

# kalo misalnya create table if not exists user_address_2018_snapshots {} tidak ada if not exists tapi cuma create table user_address_2018_snapshots
try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created successfully")
except pg.errors.DuplicateTable:
    print("table already created...")

#load zipped file to dataframe
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name),header=None)
#kalao pakai chucksize disini, performance system yang menajlanakan python code yang perlu di optimalkan

col_name_df =  [i['column_name'] for i in content]
df.columns = col_name_df

df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]

#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


#inser into postgres
df_filtered.to_sql(table_name, engine, if_exists='replace', index=False)
#kalo pakai chucksize (per batch insert table maka performance postgres yang harus di optimalkan)

print(f'total inserted rows: {len(df)}')
print(f'initial created at: {df_filtered.created_at.min()}')
print(f'Last created at: {df_filtered.created_at.max()}')

cursor.execute(open(result_ingestion_check_sql, 'r').read())
result = cursor.fetchall()
print(result)
# print(engine)
# print(df_filtered)
# print(df_filtered['created_at'].max())
# print(df['created_at'].min())
# print(df.head())
# print (col_name_df)