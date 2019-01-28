import base64
import hashlib
import json
import logging
import random

import datetime
import pandas as pd
import pytz
import time

#from adobe.constants import API_HOST, PATH, CONFIG_FILE, HANA_CONFIG_FILE, SCRIPT_FOLDER_NAME
from constants import API_HOST, PATH, CONFIG_FILE, HANA_CONFIG_FILE, SCRIPT_FOLDER_NAME
from base.base_api_interface import BaseApiInterface
from base.db_wrapper import PyHdbWrapper
from base.utils import Utils

nonce_seed = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c',
              'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
              'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C',
              'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
              'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

logger = logging.getLogger(__name__)


class AdobeBase(BaseApiInterface):
    def __init__(self, kargs):
        logger.info("class initiated")
        self.da_path = kargs['da-path']
        self.date_from = kargs['date-from']
        self.date_to = kargs['date-to']
        self.date_granularity = kargs['date-granularity']
        self.sleep_time = float(kargs['sleep-time'])
        self.schema = kargs['schema-name']

    def get_endpoint_url(self, query):
        return API_HOST + PATH + query

    def get_payload(self, dateFrom, dateTo, dateGranularity):
        return {
            'reportDescription': {
                'reportSuiteID': 'servn-servicenow.com-prod',  # servn-servicenow.com-prod 'servn-geneva-prod'
                'dateFrom': dateFrom,
                'dateTo': dateTo,
                'dateGranularity': dateGranularity,
                'locale': 'en_US',
                'metrics': [
                    # {
                    #     'id': 'event91'
                    # }
                    {
                        'id': 'event17'
                    },
                    {
                        'id': 'event89'
                    }
                ],
                    'elements':[
                        {
                            'id':'eVar48',
                            'classification':'SurfID',

                        }
                        # {
                        #     'id':'linkdownload',
                        #
                        # }
                #]

                        ],
                'segments': [
                    {
                        #'id': 's300007365_56d8b42ce4b0735cde722317'
                        'id': 's300007365_5b1ee51fbef0d34e1bda4081'

                    }
                ]
            }
        }

    def get_header(self, username, digest, nonce_b, iso_time):
        header_args = 'UsernameToken Username="' + username + \
                      '", PasswordDigest="' + digest + \
                      '", Nonce="' + nonce_b + \
                      '", Created="' + iso_time + \
                      '"'
        header_args = bytes(header_args, 'utf-8')

        return {
            'X-WSSE': header_args
        }

    def get_unique_connection_parameters(self, api_secret):

        nonce = ''.join(random.sample(nonce_seed, 16))
        nonce_b = base64.b64encode(nonce.encode('ascii'))

        date = datetime.datetime.now(tz=pytz.utc)
        date = date.astimezone(pytz.timezone('US/Pacific'))

        iso_time = date.strftime("%Y-%m-%dT%H:%M:%S")
        # iso_time = time.strftime("%Y-%m-%dT%H:%M:%S")

        # This is the process to create digest which is used in header
        passwd = nonce + iso_time + api_secret
        passwd = passwd.encode()
        hash_new = hashlib.sha1(passwd).digest()
        digest = base64.b64encode(hash_new)
        return nonce_b.decode('ascii'), iso_time, digest.decode('ascii')

    def response_handler(self, data_df):
        db_obj = PyHdbWrapper()
        cursor, connection = db_obj.connect_hana(
            Utils.get_file_path(self.da_path, [SCRIPT_FOLDER_NAME, HANA_CONFIG_FILE]), 'HANA_ENV')
        ''' Truncate staging table before inserting records'''
        delete_page_url = db_obj.get_delete_query(self.schema + '.STG_PAGE_URL_METRICS')
        db_obj.execute_sql(cursor, connection, delete_page_url, '', 'DELETE')

        # Extract Date will be used as a bookmark for loading data into HANA
        extract_date = datetime.datetime.today()

        for index, row in data_df.iterrows():  # Outer Loop for Day Specific data
            table = row.iloc[0]  # Table is of type Dictionary
            source_date = str(datetime.date(table['year'], table['month'], table['day']))
            source_date = datetime.datetime.strptime(source_date, "%Y-%m-%d")
            breakdown = (table['breakdown'])  # Breakdown is type of list
            for i in breakdown:
                if 'breakdown' in i.keys():
                    country = i['name']
                    # print(i['breakdownTotal'])
                    temp = i['breakdown']
                    for i in temp:
                        counts = i['counts']
                        pageviews = counts[0]
                        visits = counts[1]
                        uniquevisitors = counts[2]
                        bouncerate = counts[3]
                        averageTimeSpentOnSite = counts[4]
                        url = i['name']
                        print(source_date, country, url + '\n' + pageviews, visits, uniquevisitors, bouncerate)
                        column_name = ["PERIOD_DATE", "GRANULARITY", "COUNTRY", "URL"
                            , "PAGE_VIEWS_COUNT", "PAGE_VISITS_COUNT", "UNIQUE_VISITOR_COUNT"
                            , "BOUNCE_RATE_%%", "AVG_TIME_SPENT_ON_PAGE", "EXTRACT_DATE"]
                        insert_query = db_obj.get_insert_query(self.schema+".STG_PAGE_URL_METRICS", column_name)

                        values = [source_date, self.date_granularity, country, url, pageviews, visits,
                                  uniquevisitors, bouncerate, averageTimeSpentOnSite, extract_date]
                        print (values)

                        #db_obj.execute_sql(cursor, connection, insert_query, values, 'INSERT')

        upsert_statement = "UPSERT \"" + self.schema + "\".\"PAGE_URL_METRICS\"  \
                                        SELECT * FROM \"" + self.schema + "\".\"STG_PAGE_URL_METRICS\""
        #db_obj.execute_sql(cursor, connection, upsert_statement, '', 'UPSERT')

    def main(self):
        '''
        This function will be called from the main.py file and contains the
        logic to fetch data from source and will save it to designation.
        :return:
        '''

        '''
        from_ini function will read the configuration file as per given section name and key name 
        and will provide dict of configuration parameters.
        '''
        #print ("Date granularity is: {}".format(self.date_granularity))
        adobe_config = Utils.from_ini(
            Utils.get_file_path(
                self.da_path,
                [SCRIPT_FOLDER_NAME, CONFIG_FILE]),
            'Adobe_Analytics',
            ('username', 'api_secret'))

        '''
        Getting end point url 
        '''
        query_url = self.get_endpoint_url('method=Report.Queue')
        print ("\n")
        print (query_url)
        print ("\n")

        def get_visits(x):
            y = x[0]
            return y

        def get_form_success(x):
            y = x[0]
            return y

        def get_video_views(x):
            y = x[1]
            return y

        def get_return_visits(x):
            y = x[0]
            return y

        '''
        Getting payload to be passed with the api
        '''
        payload = json.dumps(self.get_payload(self.date_from, self.date_to, self.date_granularity))
        print ("------------------------------------")
        print ("Payload is:")
        print (payload)
        print ("------------------------------------")
        print ("\n")
        '''
        Preparing parameters for passing in header with api for authentication
        '''
        nonce_b, iso_time, digest = self.get_unique_connection_parameters(adobe_config['api_secret'])

        '''
        Get header
        '''
        head = self.get_header(adobe_config['username'], digest, nonce_b, iso_time)

        print ("------------------------------------")
        print ("Header is:")
        print (head)
        print ("------------------------------------")
        print ("\n")


        '''
        Calling api for preparing reports
        '''
        report_queue_api_response = Utils.send_request('POST', query_url, payload, head)

        if report_queue_api_response.status_code != 200:
            logger.error(report_queue_api_response.text)
            raise Exception(report_queue_api_response.reason)

        report_queue_response_body = report_queue_api_response.text.encode('ascii')
        temp_var = report_queue_response_body.split(b':')
        report_id = temp_var[1].replace(b'}', b'')
        # print(report_id)

        '''
        Section - 2: Get data based on report developed and save the JSON reply in shared folder 
        '''

        '''
        Developing API URL for retrieving
        '''
        query_url = self.get_endpoint_url('method=Report.Get')

        # The body of the API url is enclosed as post_params
        bodydata = {
            'reportID': '' + report_id.decode('ascii') + ''
        }
        payload = json.dumps(bodydata)

        counter_error = 0
        while (counter_error == 0):
            # Using sleep method to give enough time to get the reort ready to pull the data else it will throw
            # "Report not ready"
            print("Start sleep time " + time.strftime("%X"))
            print ("\n")
            time.sleep(self.sleep_time)

            '''
            Get connection parameter for getting reports data
            and get header
            '''
            nonce_b, iso_time, digest = self.get_unique_connection_parameters(adobe_config['api_secret'])
            head = self.get_header(adobe_config['username'], digest, nonce_b, iso_time)

            # logger

            '''
            Call api to get reports
            '''
            api_response = Utils.send_request('POST', query_url, payload, head)

            try:
                response_body = json.loads(api_response.text)
                #print ("Response Body is: {}".format(response_body))
                print ("------------------------------------")
                print ("API Response is:")
                print ("\n")
                print ("Response is: {}".format(response_body))
                print ("------------------------------------")
                print ("\n")
                with open(r'C:\Users\rajkiran.reddy\Desktop\SNow-Projects\AdobeAnalytics\Adobe_data.json', 'w') as f:
                    json.dump(response_body, f)
                # Using Pandas library to load json data and transpose it for easy manuplation
                adobe_ana_pd = pd.DataFrame.from_dict(response_body)
                adobe_ana_pd = adobe_ana_pd.T
                # Removing unwanted index from the dataFrame
                adobe_ana_pd = adobe_ana_pd.drop(adobe_ana_pd.index[1:])

                # The metrics for Adobe Analytics is in 'data' column, so parsing it
                #print (adobe_ana_pd['data'][0][0]['name'])
                #print (len(adobe_ana_pd['data'][0][0]['breakdown'][0]['counts']))
                final_data_df = pd.DataFrame(adobe_ana_pd['data'][0][0]['breakdown'])
                #print (final_data_df.columns)
                final_data_df = final_data_df.rename(columns={"counts": "Counts",
                                                "name": "Surf ID",
                                                "url": "URL"})

                final_data_df = final_data_df[["Counts", "Surf ID"]]
                final_data_df['Granularity'] = adobe_ana_pd['data'][0][0]['name']

                if len(adobe_ana_pd['data'][0][0]['breakdown'][0]['counts']) > 1:
                    final_data_df['Visits'] = final_data_df['Counts'].map(get_visits)
                    final_data_df['e17 Form Success'] = final_data_df['Counts'].map(get_form_success)
                    final_data_df['e89 Video Views'] = final_data_df['Counts'].map(get_video_views)
                else:
                    final_data_df['Return Visits + Visits'] = final_data_df['Counts'].map(get_return_visits)

                final_data_df_columns = list(final_data_df.columns)
                final_data_df_columns.remove('Counts')
                final_data_df = final_data_df[final_data_df_columns]
                print (final_data_df)

                if 'error' in response_body.keys():
                    if 'report_not_ready' in response_body['error']:
                        pass
                    else:
                        logger.error(api_response.text)
                        raise Exception(api_response.reason)
                elif 'report' in response_body.keys():
                    counter_error = 1
            except Exception as e:
                logger.error(e)
                raise


        # # Using Pandas library to load json data and transpose it for easy manuplation
        # adobe_ana_pd = pd.DataFrame.from_dict(response_body)
        # adobe_ana_pd = adobe_ana_pd.T
        #
        # # Removing unwanted index from the dataFrame
        # adobe_ana_pd = adobe_ana_pd.drop(adobe_ana_pd.index[1:])
        #
        # # The metrics for Adobe Analytics is in 'data' column, so parsing it
        # data_df = pd.read_json((adobe_ana_pd['data']).to_json())
        # #print (data_df.head())
        #
        # # datetime.datetime.strptime(str(datetime.date.today()),"%Y-%m-%d")
        #
        #
        # # call delete from table bane before processing response
        # # also add the migration from staging to main table
        #
        # # Iterating over the JSON file to extract metrics
        # self.response_handler(data_df)
