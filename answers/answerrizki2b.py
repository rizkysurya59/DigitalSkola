import json
from re import L
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json = 'sql/schemas/user_address.json'
create_schema_sql = """create table if not exists user_address_master {};"""
zip_small_file = 'temp/dataset-medium.zip'
result_ingestion_check_sql = 'sql/queries/result_ingestion_user_address_master.sql'
small_file_name = 'dataset-medium.csv'
database='shipping_orders'
user='postgres'
password='sword1st'
host='127.0.0.1'
port='5432'
table_name = 'user_address_master'


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

# list_insert = []
# for data in range(3000):
#     list_insert.append(size=5)

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


#load zipped file to dataframe
zf = ZipFile(zip_small_file)
#masukin 3000 data
df = pd.read_csv(zf.open(small_file_name),header=None,nrows=3000)


col_name_df =  [i['column_name'] for i in content]
df.columns = col_name_df

# df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]
# df_filtered = pd.DataFrame({'small_file_name':range(1,3000)})
# df_filtered = df.shape({'data-part-1: [1,3000]'})



df_filtered = df

#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


#inser into postgres
df_filtered.to_sql(table_name, engine, if_exists='append', index=False)

# catch if table already exists then fail
try:    
    cursor.execute(create_schema_sql_final)
    print('Table berhasil di buat')
except pg.errors.DuplicateTable:
    # print('biasa')
    df_filtered.to_sql(table_name, if_exists='fail', index=False)

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