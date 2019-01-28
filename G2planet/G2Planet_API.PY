from queue import Queue
from threading import Thread
from time import time
import hashlib, base64, urllib, time, logging, json, requests as rq,pandas as pd,pyhdb,os,configparser as cp,datetime
import urllib,logging
import datetime, dateutil.parser,pytz

class G2Planet_API(object):
    def __init__(self,project_id,pusk_id,api_version,vendor_name,output_field_set):
        self._project_id = project_id
        self._pusk_id = pusk_id
        self._api_version = api_version
        self._vendor_name = vendor_name
        self._output_field_set = output_field_set
        
    def get_project_id(self):
        return self._project_id    
    def set_project_id(self, value):
        self._project_id = value
        project_id = property(get_project_id, set_project_id)
        
    def get_pusk_id(self):
        return self._pusk_id    
    def set_pusk_id(self, value):
        self._pusk_id = value
        pusk_id = property(get_pusk_id, set_pusk_id)

    def get_api_version(self):
        return self._api_version    
    def set_api_version(self, value):
        self._api_version = value
        api_version = property(get_api_version, set_api_version)
        
    def get_vendor_name(self):
        return self._vendor_name    
    def set_vendor_name(self, value):
        self._vendor_name = value
        vendor_name = property(get_vendor_name, set_vendor_name)
        
    def get_output_field_set(self):
        return self._output_field_set    
    def set_output_field_set(self, value):
        self._output_field_set = value
        output_field_set = property(get_output_field_set, set_output_field_set)
        
    def build_payload(self,offset,limit,begining_timestamp,ending_timestamp):
        payload={'project_id': self.get_project_id(),
                'pusk_id': self.get_pusk_id(),
                'api_version':self.get_api_version(),
                'vendor_name': self.get_vendor_name(),
                'output_field_set': self.get_output_field_set(),
                'beginning_timestamp': begining_timestamp, 
                'ending_timestamp': ending_timestamp, 
                'output_format':'json',
                'offset':offset,
                'limit':limit
                }
        return payload
    
    def send_request(self,req_type,url,body,head):
        #Calling the API with URL,HEADER and BODY
        try:
            if req_type is 'GET':
                final_ret = rq.get(url,headers=head,params=body)
                return final_ret.text
            if req_type is 'POST':
                final_ret =  rq.post(url,data=body,headers=head)
                return final_ret
        except Exception as e:
            print(str(e))
            raise
        except rq.exceptions.Timeout as e:
            print("Timeout Error",str(e))
            raise
        except rq.exceptions.TooManyRedirects as e:
            print("redirects Error",str(e))
            raise
        except rq.exceptions.RequestException as e:
            print('Exception occured', str(e))
            raise
            
    def post(self,url,body,head):
        if not body:
            body = None
        if not head:
            head = None
        return self.send_request('POST',url,body,head)
        

        
    def get_hana_table_defination(self,cur,table_name):
        print(table_name)
        table_defination_select = "SELECT TOP 1 * FROM "+table_name
        print('**** SELECT STATEMENT - '+table_defination_select )
        #Extracting table defination
        table_defination = cur.execute(table_defination_select)
        column_names = [x[0] for x in table_defination.description] #This extracts columns names
        column_datatypes = list(table_defination._column_types) #This extracts datatypes
        return column_names,column_datatypes
    
        #Making connection with SAP HANA Database
    def connect_hana(self,da_path):
        #Building the file path to access config file
        DataAnalyticsShare_path = da_path.split("\\")
        DataAnalyticsShare_path.append('PythonScripts')
        DataAnalyticsShare_path.append('G2PLANET_CONFIG.cfg')
        DataAnalyticsShare_path = os.path.join('\\\\',*DataAnalyticsShare_path)

        #Reading the config file
        cf = cp.ConfigParser()
        cf.read(DataAnalyticsShare_path)

        hana_server=cf.get('HANA_ENV','server')#'YmloYW5hZGV2LmNvcnAuc2VydmljZS1ub3cuY29t'#
        hana_server = str(base64.b64decode(hana_server),'utf-8')
        print(hana_server)
        port=cf.get('HANA_ENV','port')#'MzAxMTU='#
        port = int(base64.b64decode(port))
        user_name=cf.get('HANA_ENV','user_name')#'U1ZDX0VCSV9EU0Q='#
        user_name = str(base64.b64decode(user_name),'utf-8')
        print(user_name)
        pwd=cf.get('HANA_ENV','pwd')#'U2VydmljZW5vdzEyMyM='#
        pwd = str(base64.b64decode(pwd),'utf-8')

        try: #Connecting to HANA
            con = pyhdb.connect(hana_server, port, user_name,pwd)
            cur = con.cursor()
            #print('Connection to HANA DB successful')
            return con,cur

        except pyhdb.DatabaseError as e:
            #print ("Database connection error",e)
            raise
            
    def remove_unwanted_columns(self,result_dict):
        #Removing all the unwanted columns which has data None
        res = {k:v for k,v in result_dict.items() if v is not None}
        return res
    
    def generate_insert_query(self,res,table_name):
        query_placeholders = ', '.join(['%s'] * len(res.values()))
        query_columns = (', '.join('"' + item + '"' for item in res.keys()))#', '.join(res.keys())
        insert_query = ''' UPSERT %s (%s) VALUES (%s) WITH PRIMARY KEY ''' %(table_name,query_columns, query_placeholders)
        return insert_query
    
    def generate_hana_store_dict(self,column_names,column_datatypes,resp_dict):
        #Creating a dictionary with K:V to store data
        #resp_dict is reposne dictionary of data
        result_dict = {}
        for x in column_names:
            if x not in resp_dict.keys():
                result_dict[x] = None
            elif isinstance(resp_dict[x], list):
                if not resp_dict[x]:
                    result_dict[x] = None
            elif isinstance(resp_dict[x], bool):
                if resp_dict[x] is True:
                    result_dict[x] = 1
                else:
                    result_dict[x] = 0
            elif 'timestamp' in x:
                if not resp_dict[x]:
                    result_dict[x] = None
                else:
                    result_dict[x] = self.convert_timezone_to_utc(resp_dict[x])
            elif column_datatypes[column_names.index(x)].__dict__.get('type_code') is 1:# Datatype code '1' is TINYINT

                #### Following IF ELSE Added by RAJKIRAN REDDY
                if resp_dict[x] is not None:
                    if (resp_dict[x] is True) or (resp_dict[x].upper() == 'TRUE'):
                        result_dict[x]=1
                    else:
                        result_dict[x]=0
                else:
                    resp_dict[x] = None
            elif column_datatypes[column_names.index(x)].__dict__.get('type_code') in [9,11,25]:                 
                if resp_dict[x] is True:
                    result_dict[X]=resp_dict[x][:4999]
                else:
                    result_dict[X]=None             
            else:
                result_dict[x] = resp_dict[x]
        result_dict['ETL_EXTRACT_DATE'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return result_dict
    
    def set_error_logging(self,filepath):
        logger = logging.getLogger('G2PLANET_log') 
        hdlr = logging.FileHandler(filepath+'\\'+'G2PLANET_log_'+str(datetime.datetime.now().strftime('%Y-%m-%d'))+'.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr) 
        logger.setLevel(logging.WARNING)
        return logger
    
    def pop_logging(self,logger,msg):
        try:
            logger.error(msg)
            #logger.info(msg)
        except :
            print('Exception in Logging')
            pass
    
    def convert_timezone_to_utc(self,time):
        d = dateutil.parser.parse(time)
        return d.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    def execute_hana_sql(self,logger,cur,con,statement,values,dml_type):
        try:
            if dml_type is 'SELECT':
                cur.execute(statement)
                return cur.fetchall()
            else:
                cur.execute(statement,values)
                con.commit()
        except pyhdb.DatabaseError as e:
            self.pop_logging(logger,("Database connection error",e,statement,values))
            pass
            #raise
        except pyhdb.OperationalError as e:
            print ("Database Operational error", e,table_name)
            pass
            #raise
        except pyhdb.IntegrityError as e:
            print ("Database Intergrity error", e,table_name)
            pass
            #raise
        except pyhdb.Error as e:
            print ("Error when trying to execute a cursor or fetch data into the cursor",e,table_name)
            pass
            #raise