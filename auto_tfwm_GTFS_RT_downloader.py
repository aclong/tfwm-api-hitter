#load in libraries
from dotenv import load_dotenv
import requests
import os
from google.transit import gtfs_realtime_pb2
import urllib
import pandas as pd
import psycopg2
import datetime as dt

#get the env variables
load_dotenv()

#get the individual variables
tfwm_api_key=os.getenv('tfwmapikey')
tfwm_api_id=os.getenv('tfwmapiid')

#set up the requests
base_url='http://api.tfwm.org.uk/'
gtfs_rt_url=f'{base_url}gtfs/trip_updates?'

#request url with codes
tfwm_req_url=f'{gtfs_rt_url}app_id={tfwm_api_id}&app_key={tfwm_api_key}'

#get the response
response=requests.get(tfwm_req_url)

if response.status_code==200:
    
    print("good response")
    
    #get google feed message
    feed = gtfs_realtime_pb2.FeedMessage()

    #get request info
    date_string=response.headers['Date']

    req_date_time=dt.datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %Z')

    #parse response content
    feed.ParseFromString(response.content)

    #create the empty dataframe
    column_names=['request_datetime','route_id', 'trip_id', 'start_date_time', 'stop_sequence','stop_id', 'arrival', 'departure' ]

    #df=pd.DataFrame(columns=column_names)
    
    if len(feed.entity)>0:
        
        print(f'compiling data from {len(feed.entity)} buses')
        
        rows_list = []
        
        #start loop to create pd
        for i in range(0,len(feed.entity)):
            
            #route id
            route_id=feed.entity[i].trip_update.trip.route_id

            #trip id
            trip_id=feed.entity[i].trip_update.trip.trip_id

            #start datetime
            start_date_time=dt.datetime.strptime(f'{feed.entity[i].trip_update.trip.start_date}_{feed.entity[i].trip_update.trip.start_time}', '%Y%m%d_%H:%M:%S')

            #then iterate through the update times
            for j in range(0, len(feed.entity[i].trip_update.stop_time_update)):

                #get all the info
                stop_seq=feed.entity[i].trip_update.stop_time_update[j].stop_sequence

                #feed.entity[1].trip_update.stop_time_update[0].stop_sequence

                stop_id=feed.entity[i].trip_update.stop_time_update[j].stop_id

                #datetime.fromtimestamp(unix_timestamp, local_timezone)
                arr=dt.datetime.fromtimestamp(feed.entity[i].trip_update.stop_time_update[j].arrival.time)

                dep=dt.datetime.fromtimestamp(feed.entity[i].trip_update.stop_time_update[j].departure.time)
                
                #create row dictionary
                
                row_dict={"request_datetime": req_date_time,
                         "route_id": route_id,
                         "trip_id": trip_id,
                         "start_date_time": start_date_time,
                         "stop_sequence":stop_seq,
                         "stop_id":stop_id,
                         "arrival":arr,
                         "departure":dep}
                
                rows_list.append(row_dict)
                
                #turn it into a df
                #df.loc[i]=
                
                #df_new=pd.DataFrame([req_date_time, route_id, trip_id, start_date_time, stop_seq,stop_id,arr,dep], columns=column_names)

                #write all the data into the df
                #df.append(df_new, ignore_index=True)
    
        df = pd.DataFrame(rows_list, columns=column_names) 
        
        #remove false arrival and departure times
        df.loc[df['arrival'] < dbyday, 'arrival' ] = pd.NaT
        df.loc[df['departure'] < dbyday, 'departure' ] = pd.NaT
    
    else:
        print("no returned data")
else:
    print("bad request")