from sqlalchemy import create_engine
from minio import Minio
import pandas as pd
import duckdb
import os
import io
from datetime import datetime, timedelta
import psycopg2

# ESTABLISH CONNECTIONS
postgres_conn = psycopg2.connect(user="oltp",
                                  password="oltp",
                                  host="localhost",
                                  port="5432",
                                  database="oltp")

conn = duckdb.connect('~/my-first-elt-project/docker/db/duckdb/duckdb_dw.duckdb')

# Right here I need to put all these credentials into a seperate file
client = Minio(endpoint="localhost:9000", access_key='a5926TSNVC2r9J4Y2Eqh', 
               secret_key='3YBQqcerjz5TsV8X851gi3Rl7YNclYQ6UD1MrEPY', secure=False)

engine = create_engine(
    'postgresql+psycopg2://oltp:oltp@localhost:5432/oltp'
)
#--------------------- AGAIN, THESE SHOULD BE DEFINED SEPARATELY-------------------
#DEFINITIONS
tbl_list = ['users', 'products', 'transactions', 'transaction_detail']
bucket_name = 'snapshot'

#DEFINE CURRENT TIME VARIABLES
current_dt_string = datetime.now()
current_date_full = current_dt_string.strftime("%d%m%Y%H%M%S")
current_year = current_dt_string.strftime("%Y")
current_month = current_dt_string.strftime("%m")
current_date = current_dt_string.strftime("%d")

#DEFINE PREVIOUS DAY TIME VARIABLES

prev_dt_string = current_dt_string - timedelta(days=1)
prev_full_date = prev_dt_string.strftime("%d%m%Y%H%M%S")
prev_year = prev_dt_string.strftime("%Y") # prev_year means that the year that last day's data was on, not literally 'last year'
prev_month = prev_dt_string.strftime("%m") # the same rule applies for prev_month and prev_date.
prev_date = prev_dt_string.strftime("%d")

#DEFINE T-2 DAY TIME VARIABLES
t2_dt_string = prev_dt_string - timedelta(days=1)
t2_full_date = prev_dt_string.strftime("%d%m%Y%H%M%S")
t2_year = prev_dt_string.strftime("%Y") # prev_year means that the year that last day's data was on, not literally 'last year'
t2_month = prev_dt_string.strftime("%m") # the same rule applies for prev_month and prev_date.
t2_date = prev_dt_string.strftime("%d")



def create_df_for_t1_day(table_name):
    df_t1 = pd.DataFrame()
    table = table_name
    # for table in tbl_list:
        # t1_dt_obj_list = client.list_objects(bucket_name=bucket_name, prefix=f'{bucket_name}/{table}/2024/06/08/')
    t1_dt_obj_list = client.list_objects(bucket_name=bucket_name, prefix=f'{bucket_name}/{table}/{prev_year}/{prev_month}/{prev_date}/')
    for obj in t1_dt_obj_list:            
        file_path = os.path.basename(obj.object_name)
        object_key = obj.object_name
        print(f'object_key: {object_key}, file_path: {file_path}')

        try:
            response_t1 = client.get_object(bucket_name,object_name=object_key)
            buffer = io.BytesIO(response_t1.read())
            df = pd.read_parquet(buffer)
            df_t1 = pd.concat([df_t1, df], ignore_index=True) #.reset_index(drop=True)
            print(df_t1)       
        
        finally:
            response_t1.close()
            buffer.close()
            response_t1.release_conn()
    return df_t1


def create_df_for_t2_day(table_name):
    df_t2 = pd.DataFrame()
    table = table_name
    # for table in tbl_list:
    t2_dt_obj_list = client.list_objects(bucket_name=bucket_name, 
                                         prefix=f'{bucket_name}/{table}/{t2_year}/{t2_month}/{t2_date}/')
    for obj in t2_dt_obj_list:            
        file_path = os.path.basename(obj.object_name)
        object_key = obj.object_name
        print(f'object_key: {object_key}, file_path: {file_path}')

        try:
            response_t2 = client.get_object(bucket_name, object_name=object_key)
            buffer = io.BytesIO(response_t2.read())
            df = pd.read_parquet(buffer)
            df_t2 = pd.concat([df_t2, df], ignore_index=True)
            print(df_t2)       
            
        finally:
            response_t2.close()
            buffer.close()
            response_t2.release_conn()
    return df_t2


def create_df_for_changes(table_name):
    tbl = table_name
    df1 = create_df_for_t1_day(tbl)
    df2= create_df_for_t2_day(tbl)
    changes = df1[~df1.apply(tuple, 1).isin(df2.apply(tuple, 1))]
    return changes

def get_primary_key_columns(table_name):
    query = f"""
    SELECT a.attname as key_name
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                        AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = '{table_name}'::regclass
    AND    i.indisprimary
    """
    cur = postgres_conn.cursor()
    cur.execute(query)
    col_tuple = cur.fetchall() #this returns a list of tuples [('id',), ('name',), ('email',), ('created_at',)]
    col_list = [t[0] for t in col_tuple]
    return col_list


def upsert_sql(df, table_name):
    a = []
    target_table_name = table_name
    temp_table = f"{table_name}_temporary_table"
    key_name = get_primary_key_columns(table_name)
    for col in df.columns:
        if col in key_name:
            continue
        a.append(f'"{col}"=EXCLUDED."{col}"')
    conn.execute(f"CREATE TABLE IF NOT EXISTS {temp_table} AS SELECT * FROM df")
    conn.execute(f"INSERT INTO {temp_table} SELECT * FROM df")
    
    #df.to_sql(temp_table, engine, if_exists='replace', index=False) 
    set_statement = ', '.join(a)
    upsert_query = f"""
    WITH primary_col_extract AS (
        SELECT a.attname as key_name
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = {target_table_name}::regclass
        AND    i.indisprimary
    )
    INSERT INTO {target_table_name} as tt 
    SELECT * FROM {temp_table} as tmp
    ON CONFLICT
    DO UPDATE SET """
    # combined_query = upsert_query
    print(upsert_query + set_statement)
    conn.execute(upsert_query + set_statement)
    
    conn = duckdb.connect('~/my-first-elt-project/docker/db/duckdb/duckdb_dw.duckdb')


def create_fact_table():
    conn.execute(""" CREATE TABLE IF NOT EXISTS facts (
                        id VARCHAR(255) PRIMARY KEY,
                        transaction_date TIMESTAMP NOT NULL,
                        total_amount FLOAT NOT NULL,
                        cash_received FLOAT NOT NULL,
                        change_due FLOAT NOT NULL,
                        user_id VARCHAR(255) REFERENCES users(id),
                        product_id VARCHAR(255) REFERENCES products(id),
                        quantity INTEGER NOT NULL,
                        item_amount FLOAT NOT NULL
                    )               
                    """)
    return


# def insert_into_fact_table(df):
#     fact_df = df
#     conn.execute(""" INSERT INTO facts(id,user_id,product_id,transaction_date,total_amount,item_amount,quantity,cash_received,change_due)
#                  SELECT id,user_id,product_id,transaction_date,fd.total_amount_x,fd.total_amount_y,quantity,cash_received,change_due FROM fact_df fd
                 
#                  """)
#     return


def insert_into_facts():
    for t in tbl_list:
        if t in ['users', 'products']:
            change_df = create_df_for_changes(t)
            upsert_sql(change_df,t)
        
        elif t == 'transactions':
            trans_df = create_df_for_changes(t)
        elif t == 'transaction_detail':
            details_df = create_df_for_changes(t)
            facts = pd.merge(details_df, trans_df, how='inner', on=[details_df['transaction_id'], trans_df['id']])
            print(facts)
            conn.execute(""" INSERT INTO 
                         facts(id,user_id,product_id,transaction_date,total_amount,item_amount,quantity,cash_received,change_due)
                        SELECT id,user_id,product_id,transaction_date,fd.total_amount_x,fd.total_amount_y,quantity,cash_received,change_due 
                         FROM facts fd
                    """)


def elt_process():
    with open('/home/admin/my-first-elt-project/docker/db/oltp_schema.sql', 'r') as file:
        sql_script = file.read()
    conn.execute(sql_script)
    create_fact_table()
    insert_into_facts()
    conn.close()
    print('data has been upserted successfully')
    
    
    # for t in tbl_list:
    #     if t in ['users', 'products']:
    #         change_df = create_df_for_changes(t)
    #         upsert_sql(change_df,t)
        
    #     elif t == 'transactions':
    #         trans_df = create_df_for_changes(t)
    #     elif t == 'transaction_detail':
    #         details_df = create_df_for_changes(t)
    #         facts = pd.merge(details_df, trans_df, how='inner', on=[details_df['transaction_id'], trans_df['id']])
    #         print(facts)
    #         insert_into_fact_table(facts)
    


