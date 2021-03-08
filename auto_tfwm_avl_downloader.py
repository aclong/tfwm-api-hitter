#!/opt/anaconda/bin/python


#load in libraries
from dotenv import load_dotenv
import requests
import os
from google.transit import gtfs_realtime_pb2
import urllib
import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
import datetime as dt

#get the env variables
load_dotenv()

#set up db connection and passing pd to database function
# Here you want to change your database, username & password according to your own values
credential_dic = {
    "host"      : os.getenv('dbhost'),
    "database"  : os.getenv('dbname'),
    "user"      : os.getenv('dbuser'),
    "password"  : os.getenv('dbpass')
}

db_schema=os.getenv('dbschemaname')

#create the connection using your credentials
def connect(credential_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**credential_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

#copy from stringio version of getting db to table
#messy datetime conversion going on in here - convert all to strings is probably for the best
def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    
    
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

#set up the api requests variables
#get the api variables
tfwm_api_key=os.getenv('tfwmapikey')
tfwm_api_id=os.getenv('tfwmapiid')

#set up the requests
base_url='http://api.tfwm.org.uk/'
gtfs_rt_avl_url=f'{base_url}gtfs/vehicle_positions?'

#request url with codes
tfwm_req_url=f'{gtfs_rt_avl_url}app_id={tfwm_api_id}&app_key={tfwm_api_key}'

#get the response
response=requests.get(tfwm_req_url)

if response.status_code==200:
    
    print("good response")
    
    #get google feed message
    feed = gtfs_realtime_pb2.FeedMessage()

    #get request info
    date_string=response.headers['Date']

    req_date_time=dt.datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %Z')
    
    time_zone_string=dt.datetime.now(dt.timezone.utc).astimezone().tzname()
    
    #parse response content
    feed.ParseFromString(response.content)
    
    if len(feed.entity)>0:
        
        print(f'compiling data from {len(feed.entity)} buses')
        
        rows_list = []
        
        #start loop to create pd
        for i in range(0,len(feed.entity)):
            
            row_dict={"request_datetime": req_date_time,
                      "route_id": feed.entity[i].vehicle.trip.route_id,
                      "trip_id": feed.entity[i].vehicle.trip.trip_id,
                      "schedule_relationship": feed.entity[i].vehicle.trip.schedule_relationship,
                      "start_date_time": f'{feed.entity[i].vehicle.trip.start_date}T{feed.entity[i].vehicle.trip.start_time}{time_zone_string}',
                      "latitude": feed.entity[i].vehicle.position.latitude,
                      "longitude": feed.entity[i].vehicle.position.longitude,
                      "current_stop_seq": feed.entity[i].vehicle.current_stop_sequence,
                      "current_status": feed.entity[i].vehicle.current_status,
                      "capture_date_time": feed.entity[i].vehicle.timestamp,
                      "vehicle_id": feed.entity[i].vehicle.vehicle.id}
                
            rows_list.append(row_dict)
            
    
        #convert rows list to dataframe
        df = pd.DataFrame(rows_list, columns=row_dict.keys()) 
        
        #convert all the date times datetime objects then to strings and add timezone info - running this in the pd 
        #vectorises all the messy conditionals that would be needed if doing at the dicitonary creation phases
        #by using panda methods
        df.start_date_time=pd.to_datetime(df.start_date_time, format='%Y%m%dT%H:%M:%S%Z', errors='coerce')
        
        df.capture_date_time=pd.to_datetime(df.capture_date_time, unit='s', origin='unix')
        
        #convert the datetimes to string and include NULL to help writing
        #.dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', 'NULL')
        df.start_date_time=df.start_date_time.dt.strftime('%Y-%m-%dT%H:%M:%S')#.replace('NaT', 'NULL')
        df.start_date_time=df.start_date_time.replace(np.nan, '-infinity')
        
        df.capture_date_time=df.capture_date_time.dt.strftime('%Y-%m-%dT%H:%M:%S')#.replace('NaT', 'NULL')
        df.capture_date_time=df.capture_date_time.replace(np.nan, '-infinity')
        
        df.request_datetime=df.request_datetime.dt.strftime('%Y-%m-%dT%H:%M:%S')#.replace('NaT', 'NULL')
        df.request_datetime=df.request_datetime.replace(np.nan, '-infinity')
    
        #now append to the database table
        
        #create connection to db
        conn = connect(credential_dic)
        
        #insert data into datatable
        copy_from_stringio(conn, df, f'{db_schema}.avl_data')
        
        
    else:
        print("no returned data")
else:
    print("bad request")