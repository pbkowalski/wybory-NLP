from sqlitedict import SqliteDict
import re
from google.cloud import storage
import dotenv
import os
from google.cloud.sql.connector import Connector
from google.auth import compute_engine
import pymysql.cursors


dotenv.load_dotenv()
connection_name = os.getenv("Google_cloud_sql_connection_name")
database_name = os.getenv("Google_cloud_sql_database_name")
database_user = os.getenv("Google_cloud_sql_database_user")
database_password = os.getenv("Google_cloud_sql_database_password")

tablenames = SqliteDict.get_tablenames('db.sqlite')
tablenames = [t for t in tablenames if t != 'unnamed']
db_posiedzenie = SqliteDict('./db.sqlite',tablename=tablenames[0])
headers = db_posiedzenie[0].keys()
types = ["CHAR(10)","TEXT", "TEXT", "TEXT", "TEXT", "TINYINT", "TINYINT", "SMALLINT", "TEXT"]
typesdict = {list(headers)[i]:types[i] for i in range(len(headers))}


# Create a Google Cloud SQL connection using a service account
credentials = compute_engine.Credentials()
connector = Connector()
conn = connector.connect(instance_connection_string=connection_name, 
                            db=database_name,
                            user=database_user,
                            password=database_password,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor,
                            driver = 'pymysql',
                            autocommit=True,
                            local_infile=True)

cursor = conn.cursor()
for table_name in tablenames:
    db_posiedzenie = SqliteDict('./db.sqlite',tablename=table_name)
    for i in range(len(db_posiedzenie)):
        if not cursor.execute("SELECT * FROM {table_name} WHERE nr_wypowiedzi = {id}".format(table_name=table_name, id=i)):
            dict_repr = db_posiedzenie[i].copy()
            #dict_repr['tekst'].replace("'",'"')
            keywords = db_posiedzenie[i]['keywords']
            if isinstance(keywords, dict):
                keywords = keywords.get('keywords', None)
            if isinstance(keywords, list):
                keywords = ','.join(keywords)
            if keywords:
                kw_as_list = keywords.split(',')
                kw_cleaned = [re.sub(r'[^\w\s]','',x.replace('\\n','')).strip()  for x in kw_as_list if re.search('\w{4,}',x)]
                dict_repr['keywords'] = ','.join(kw_cleaned)
            else:
                dict_repr['keywords'] = ''
            columns = ', '.join(dict_repr.keys())
            values = tuple(dict_repr.values())
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES"
            insert_query = insert_query + " (" + "%s,"*(len(values)-1) + "%s)"
            print(insert_query)
            cursor.execute(insert_query, values )
        else:
            print("Already exists")

conn.close()