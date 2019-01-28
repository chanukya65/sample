
###############################################################################################################################################
        # Program  Name              :    G2Planet Attendee Information Extract
        # Development Request #      :    To Load the data from Attendees json file of G2planet
        # Date Created               :    11/27/2017
        # Developed By               :    Naveen Sanka/Kiran Chinthaparthy
        # Brief Description          :    All the attendedd information who are registering for K18 are being captured by marketing team.
        #                                As Part of this requirement we are developing a Python script which does the API integration and load 
        #                                the attendee data to the Staging table. 
        # -------------------------------------------------------------------------------------------------------------------------------
        # REVISION HISTORY: 
        #--------------------------------------------------------------------------------------------------------------------------------
        #
        #
        #
        ###############################################################################################################################################

 
#!/usr/bin/python
import pyhdb
import requests
from requests.auth import HTTPBasicAuth
from urllib.request import urlopen
from datetime import datetime
from pytz import timezone
import pytz
import datetime
import json
import base64,os,sys
import configparser as cp
import pandas as pd
import G2Planet_API
#from G2Planet_API import G2Planet_API


DA_path = sys.argv[1] #'\\corp.service-now.com\DataAnalyticsShare\Dev'#
offset = int(sys.argv[2]) #'Offset is always starting with 0'
begining_timestamp = sys.argv[3]#'2017-01-01 00:00:00-0800'
ending_timestamp = sys.argv[4]#'2017-12-05 00:00:00-0800'
endpoint_url = sys.argv[5]#'https://servicenow.g2planet.com/servicenowknowledge18/api_attendee_updates.php'
table_name = sys.argv[6]#'G2PLANET.'+'ATTENDEE'
#print(DA_path,offset ,begining_timestamp ,ending_timestamp ,endpoint_url ,table_name)
#filepath = sys.argv[7]
limit = 1 # Restricting to 1 to just get the total number of records

#Building the file path to access config file
DataAnalyticsShare_path = DA_path.split("\\")
DataAnalyticsShare_path.append('PythonScripts')
DataAnalyticsShare_path.append('G2PLANET_CONFIG.cfg')
DataAnalyticsShare_path = os.path.join('\\\\',*DataAnalyticsShare_path)
#print(DataAnalyticsShare_path)

#Filepath for logger file
filepath = DA_path.split("\\")
filepath.append('G2Planet')
filepath.append('PythonLogs')
filepath = os.path.join('\\\\',*filepath)



#Reading the config file
cf = cp.ConfigParser()
cf.read(DataAnalyticsShare_path)
        
#Default API credentials
project_id = cf.get('G2Planet','project_id')
project_id = str(base64.b64decode(project_id),'utf-8')
pusk_id = cf.get('G2Planet','pusk_id')
pusk_id = str(base64.b64decode(pusk_id),'utf-8')
api_version = cf.get('G2Planet','api_version')
api_version = str(base64.b64decode(api_version),'utf-8')
vendor_name = cf.get('G2Planet','vendor_name')
vendor_name = str(base64.b64decode(vendor_name),'utf-8')
output_field_set = cf.get('G2Planet','output_field_set')
output_field_set = str(base64.b64decode(output_field_set),'utf-8')

api_details = G2Planet_API.G2Planet_API(project_id,pusk_id,api_version,vendor_name,output_field_set)
con,cur = api_details.connect_hana('\\'+DA_path)

#Set error logging
logger = api_details.set_error_logging(filepath)

#Getting HANA Table defination
column_names,column_datatypes = api_details.get_hana_table_defination(cur,table_name)

#Knowing the offset
payload = api_details.build_payload(offset,limit,begining_timestamp,ending_timestamp)
api_response = api_details.post(url=endpoint_url,body=payload,head=None)
api_response = json.loads(api_response.text)
print(api_response)
api_response_df = pd.DataFrame(api_response)
api_response_df.to_csv(r'C:\Users\chanukya.konduru\Documents\sample.txt')

limit=500 # Resetting the limit to 1000

offset = api_response['hana_reg']['records_available']//limit
final_df = pd.DataFrame()

while(offset>=0):
    payload = api_details.build_payload(offset*500,limit,begining_timestamp,ending_timestamp)
    api_response = api_details.post(url=endpoint_url,body=payload,head=None)
    api_response = json.loads(api_response.text)
    if api_response['hana_reg']['status'] is not 'error':
        print(offset)
        inner_df = pd.DataFrame(api_response['hana_reg']['attendees'])
        final_df = pd.concat([final_df,inner_df], axis=0)
        #print (final_df.head())
        for resp_dict in api_response['hana_reg']['attendees']:
            if isinstance(resp_dict['checkin_timestamp'],bool): ##[NS]Added for Bug Fix CHG0319463
                resp_dict['checkin_timestamp']=None ##[NS]Added for Bug Fix CHG0319463
            resp_dict['project_id'] = api_response['hana_reg']['request_values']['project_id']
            resp_dict = {k: None if not v else v for k, v in resp_dict.items()}   #### Added by RAJKIRAN REDDY
            result_dict = api_details.generate_hana_store_dict(column_names,column_datatypes,resp_dict)
            res = api_details.remove_unwanted_columns(result_dict)
            #insert_query = api_details.generate_insert_query(res,table_name)
            #api_details.execute_hana_sql(logger,cur,con,insert_query, list(res.values()),'UPSERT')
        offset-=1       
    else:
        api_details.pop_logging(logger,(resp,offset,table_name))
        offset-=1
final_df.to_csv(r'C:\Users\chanukya.konduru\Documents\sample.csv', index=False)
#con.close()
print('Number of records processed in this batch: '+str(api_response['hana_reg']['records_available']))