import pandas as pd
import psycopg2
from datetime import datetime
import os

# script to ingest data to local running postgres server

# inserting store_status data in postgres

store_status_df = pd.read_csv('store status.csv')

store_status_df['store_id'] = store_status_df['store_id'].apply(str)
store_status_df['status'] = store_status_df['status'].apply(lambda x: x=='active')

page_size = 300
conn = psycopg2.connect(
    user=os.getenv('postgres_user'),
    password=os.getenv('postgres_password'),
    host='localhost',
    database='postgres'
)

def get_timestamp(s):
    try:
        timestamp = datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f UTC')
    except ValueError:
        timestamp = datetime.strptime(s, '%Y-%m-%d %H:%M:%S UTC')
    return timestamp

store_status_df['timestamp_utc'] = store_status_df['timestamp_utc'].apply(get_timestamp)


def push(l, cursor):
    args = ','.join(cursor.mogrify("(%s,%s,%s)", i).decode('utf-8')
                    for i in l)
    cursor.execute("""
    INSERT INTO store_status(store_id, status, timestamp_utc)
    VALUES """ + args)

cur_list = []
with conn.cursor() as cursor:
    for i in range(store_status_df.shape[0]):
        cur_list.append((store_status_df.iloc[i]['store_id'], str(store_status_df.iloc[i]['status']), store_status_df.iloc[i]['timestamp_utc']))
        if len(cur_list)==page_size:
            push(cur_list, cursor)
            cur_list = []
    if len(cur_list)>0:
        push(cur_list, cursor)
conn.commit()


# inserting menu hours data in postgres

menu_hours_df = pd.read_csv('Menu hours.csv')

menu_hours_df['store_id'] = menu_hours_df['store_id'].apply(str)
menu_hours_df['start_time_local'] = menu_hours_df['start_time_local'].apply(lambda x: datetime.strptime(x, '%H:%M:%S'))
menu_hours_df['end_time_local'] = menu_hours_df['end_time_local'].apply(lambda x: datetime.strptime(x, '%H:%M:%S'))

def push(l, cursor):
    args = ','.join(cursor.mogrify("(%s,%s,%s, %s)", i).decode('utf-8')
                    for i in l)
    cursor.execute("""
    INSERT INTO menu_hours(store_id, day, start_time_local, end_time_local)
    VALUES """ + args)

cur_list = []
with conn.cursor() as cursor:
    for i in range(menu_hours_df.shape[0]):
        cur_list.append((menu_hours_df.iloc[i]['store_id'], str(menu_hours_df.iloc[i]['day']), menu_hours_df.iloc[i]['start_time_local'], menu_hours_df.iloc[i]['end_time_local']))
        if len(cur_list)==page_size:
            push(cur_list, cursor)
            cur_list = []
    if len(cur_list)>0:
        push(cur_list, cursor)
conn.commit()


# inserting timezones data in postgres

timezones_df = pd.read_csv('timezone.csv')
timezones_df['store_id'] = timezones_df['store_id'].apply(str)

def push(l, cursor):
    args = ','.join(cursor.mogrify("(%s,%s)", i).decode('utf-8')
                    for i in l)
    cursor.execute("""
    INSERT INTO timezones(store_id, timezone_str)
    VALUES """ + args)

cur_list = []
with conn.cursor() as cursor:
    for i in range(timezones_df.shape[0]):
        cur_list.append((timezones_df.iloc[i]['store_id'], timezones_df.iloc[i]['timezone_str']))
        if len(cur_list)==page_size:
            push(cur_list, cursor)
            cur_list = []
    if len(cur_list)>0:
        push(cur_list, cursor)
conn.commit()