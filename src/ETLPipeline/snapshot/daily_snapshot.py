import pandas as pd
import pyarrow.parquet as pq
import io
import pyarrow as pa
import psycopg2
from minio import Minio
from datetime import datetime, timedelta

client = Minio(endpoint="localhost:9000", access_key='jFESTreCHrpQqi3cZNmM', secret_key='X44op0kaOaysolQvihUXd0dDvbqAiyAdfPSjOnS3', secure=False)

conn = psycopg2.connect(user="oltp",
                                  password="oltp",
                                  host="localhost",
                                  port="5432",
                                  database="oltp")


tbl_list = ['users', 'products', 'transactions', 'transaction_detail']


# Define time variables
current_dt = datetime.now()
prev_dt_string = datetime.now() - timedelta(days=1)
full_date = prev_dt_string.strftime("%d%m%Y%H%M%S")
year = prev_dt_string.strftime("%Y")
month = prev_dt_string.strftime("%m")
date = prev_dt_string.strftime("%d")


def check_if_bucket_exists():
    if client.bucket_exists("snapshot"):
        print("snapshot bucket exists")
    else:
        client.make_bucket("snapshot")


def load_to_minio_bucket():
    for name in tbl_list:
        # activate the cursor
        cur = conn.cursor()
        # retrieve all records from current table
        cur.execute(f"SELECT * FROM {name}")
        records = cur.fetchall()
        # retrieve column names of the table
        cur.execute(f"""SELECT a.attname FROM pg_attribute a 
                    WHERE attrelid = '{name}'::regclass
                    AND NOT a.attisdropped 
                    AND a.attnum > 0 """) 
        col_tuple = cur.fetchall() #this returns a list of tuples [('id',), ('name',), ('email',), ('created_at',)]
        col_list = [t[0] for t in col_tuple]
    
        # create a dataframe from the records and col names
        df = pd.DataFrame(data=records, columns=col_list)
    
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        
        # load the dataframe into minio bucket as parquet files
        target_bucket = 'snapshot'
        target_file = f"{name}_snapshot_{full_date}.parquet"
        target_path = f"/{target_bucket}/{name}/{year}/{month}/{date}/{target_file}"
        
        client.put_object(target_bucket,target_path, buffer, 
                    length=buffer.getbuffer().nbytes, content_type='application/parquet')
        cur.close()
    
if __name__=="__main__":
    check_if_bucket_exists()
    load_to_minio_bucket()
