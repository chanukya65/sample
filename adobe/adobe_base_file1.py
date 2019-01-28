import base64
import hashlib
import json
import logging
import random

import datetime
import pandas as pd
import pytz
import time
from dateutil.parser import parse
import numpy as np
from pandas.io.json import json_normalize

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
        # self.metrics = kargs['metric-list']
        # self.elements = kargs['element-list']
        # self.element_names = kargs['element-names']
        # self.segments = kargs['segment-list']
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
                    {
                        'id': 'uniquevisitors'
                    },
                    {
                        'id': 'visits'
                    },
                    {
                        'id': 'pageviews'
                    },
                    {
                        'id': 'bounces'
                    },
                    {
                        'id': 'cm5509_576af356408496930352c481'
                    },
                    {
                        'id': 'event89'
                    },
                    {
                        'id': 'event17'
                    },
                    {
                        'id': 'cm5509_59e1193408051050b2f3ec64'
                    },
                    {
                        'id': 'totalvisitorsweekly'
                    },
                    {
                        'id': 'entries'
                    },
                    {
                        'id': 'totaltimespent'
                    },

                ],
                    'elements':[

                        {
                            'id': 'eVar48',
                            'classification': 'SurfID',

                        },

                        {
                            'id': 'evar2',
                            # 'classification': 'Page URL',
                            'name': 'Page URL c02'

                        },

                        # {
                        #     'id': 'eVar46',
                        #     'classification': 'Company Name',
                        #
                        # },
                        # {
                        #     'id': 'lasttouchchannel',
                        #     # "name": "Last Touch Marketing Channel"
                        #
                        # },
                        # {
                        #     'id': 'eVar48',
                        #     'classification': 'SurfID',
                        #
                        # },
                        # {
                        #     'id':'evar2',
                        #     #'classification': 'Page URL',
                        #     'name': 'Page URL c02'
                        #
                        # },
                        # {
                        #     'id': 'evar51',
                        #      'name': 'Internal Search Term c51',
                        # },
                        # {
                        #     'id': 'evar19',
                        #     'name': 'Internal Campaign ID c19',
                        # }
                #]

                        ],
                'segments': [
                    # {
                    #     #'id': 's300007365_56d8b42ce4b0735cde722317'
                    #     'id': 's300007365_5b1ee51fbef0d34e1bda4081'
                    # },
                    # {
                    #     'id':'s300007365_5c241d2b120ebf0fa5ab523e',
                    # },
                    {
                        'id': 's300007365_5c241d4e9bfec133d16ebefd'
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
        delete_page_url = db_obj.get_delete_query(self.schema + '.STG_ADOBE_CSC_DAILY')
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
                    # for i in breakdown:
                    #     if 'breakdown' in i.keys():
                    #         country = i['name']
                    #         # print(i['breakdownTotal'])
                    #         temp = i['breakdown']
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
                        insert_query = db_obj.get_insert_query(self.schema + ".STG_ADOBE_CSC_DAILY",
                                                               column_name)

                        values = [source_date, self.date_granularity, country, url, pageviews, visits,
                                  uniquevisitors, bouncerate, averageTimeSpentOnSite, extract_date]
                        print(values)


                        db_obj.execute_sql(cursor, connection, insert_query, values, 'INSERT')

        upsert_statement = "UPSERT \"" + self.schema + "\".\"ADOBE_CSC_DAILY\"  \
                                        SELECT * FROM \"" + self.schema + "\".\"STG_ADOBE_CSC_DAILY\""
        db_obj.execute_sql(cursor, connection, upsert_statement, '', 'UPSERT')

    #
    # def recursive_fun(self, element):
    #     breakdowndf = pd.concat([breakdowndf, pd.DataFrame(element["breakdown"])], ignore_index=True)

    # def iterdict(d):
    #     for k, v in d.items():
    #         if isinstance(v, list):
    #             iterdict(v)
    #         else:
    #             print (k, ":", v)

    def iterdict(self,d, breakdowndf):
        print ("Data is:{}".format(d))
        for element in d:
            #if element.has_key('breakdown'):
            #print("Element is: {}".format(element))
            breakdowndf = pd.concat([breakdowndf, pd.DataFrame(element)], ignore_index=True)
            if "breakdown" in element:
                breakdowndf = pd.concat([breakdowndf, pd.DataFrame(element["breakdown"])], ignore_index=True)
                for list_element in element["breakdown"]:
                    if isinstance(list_element.get("breakdown",None), list):
                        #print (list_element["breakdown"])
                        self.iterdict(list_element["breakdown"], breakdowndf)
            #print (breakdowndf)

        return breakdowndf


    # def date_conversion(self, column_value):
    #     if (column_value is not None) and (column_value is not '') and (column_value is not pd.np.nan):
    #         column_value_list = list(column_value)
    #         if column_value_list[-6] == '.':
    #             temp_datetime = datetime.datetime.strptime(column_value, '%a. %d %b. %Y').strftime('%Y-%m-%d')
    #         else:
    #             temp_datetime = datetime.datetime.strptime(column_value, '%a. %d %b %Y').strftime('%Y-%m-%d')
    #         return temp_datetime
    #     else:
    #         return None

    def date_conversion(self, column_value):
        if (column_value is not None) and (column_value is not '') and (column_value is not pd.np.nan):

            value = column_value.split(' - ')
            column_value_list_start = list(value[0])
            if column_value_list_start[-6] == '.':
                temp_datetime = datetime.datetime.strptime(value[0], '%a. %d %b. %Y').strftime('%Y-%m-%d')

            else:
                temp_datetime = datetime.datetime.strptime(value[0], '%a. %d %b %Y').strftime('%Y-%m-%d')

            return temp_datetime

        else:
            return None


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
        query_url = self.get_endpoint_url('method=Report.Get') # 'method=Report.GetMetrics'

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
            if api_response.status_code != 200:
                print(api_response)
                continue

            try:
                response_body = json.loads(api_response.text)
                print ("------------------------------------")
                print ("API Response is:")
                print ("\n")
                print ("Response is: {}".format(response_body))
                print ("------------------------------------")
                print ("\n")


                metricsdf = pd.DataFrame(response_body["report"]["metrics"])
                datadf = pd.DataFrame(response_body["report"]["data"])
                outerdf = pd.DataFrame()
                #breakdowndf_2 = pd.DataFrame()

                for breakdown_data in response_body["report"]["data"]:
                    for element in breakdown_data["breakdown"]:
                        innerdf = pd.DataFrame()
                        final_data = {}
                        final_data["ITEM"] = []
                        final_data["ACCOUNT_NUMBER"] = []
                        final_data["UNIQUE_VISITORS"] = []
                        final_data["VISITS"] = []
                        final_data["PAGE_VIEWS"] = []
                        final_data["BOUNCES"] = []
                        final_data["TIME_SPENT_ON_PAGE_(MIN)"] = []
                        final_data["E89_VIDEO_VIEWS"] = []
                        final_data["E17_FORM_SUCCESS"] = []
                        final_data["FORM_SUBMISSIONS"] = []
                        final_data["TOTAL_WEEKLY_UNIQUE_VISITORS"] = []
                        final_data["ENTRIES"] = []
                        final_data["TOTAL_TIME_SPENT"] = []
                        final_data["START_DATE_OF_WEEK"] = []
                        final_data["GRANULARITY"] = []
                        final_data["START_DATE_OF_WEEK"].append(breakdown_data["name"])
                        final_data["GRANULARITY"].append(self.date_granularity)
                        final_data["ACCOUNT_NUMBER"].append(element["name"])
                        if "breakdown" in element:
                            for pageurl in element["breakdown"]:
                                final_data["ITEM"].append(pageurl["name"])
                                final_data["UNIQUE_VISITORS"].append(pageurl["counts"][0])
                                final_data["VISITS"].append(pageurl["counts"][1])
                                final_data["PAGE_VIEWS"].append(pageurl["counts"][2])
                                final_data["BOUNCES"].append(pageurl["counts"][3])
                                final_data["TIME_SPENT_ON_PAGE_(MIN)"].append(pageurl["counts"][4])
                                final_data["E89_VIDEO_VIEWS"].append(pageurl["counts"][5])
                                final_data["E17_FORM_SUCCESS"].append(pageurl["counts"][6])
                                final_data["FORM_SUBMISSIONS"].append(pageurl["counts"][7])
                                final_data["TOTAL_WEEKLY_UNIQUE_VISITORS"].append(pageurl["counts"][8])
                                final_data["ENTRIES"].append(pageurl["counts"][9])
                                final_data["TOTAL_TIME_SPENT"].append(pageurl["counts"][10])

                        innerdf["ITEM"] = final_data["ITEM"]
                        innerdf["UNIQUE_VISITORS"] = final_data["UNIQUE_VISITORS"]
                        innerdf["VISITS"] = final_data["VISITS"]
                        innerdf["PAGE_VIEWS"] = final_data["PAGE_VIEWS"]
                        innerdf["BOUNCES"] = final_data["BOUNCES"]
                        innerdf["TIME_SPENT_ON_PAGE_(MIN)"] = final_data["TIME_SPENT_ON_PAGE_(MIN)"]
                        innerdf["E89_VIDEO_VIEWS"] = final_data["E89_VIDEO_VIEWS"]
                        innerdf["E17_FORM_SUCCESS"] = final_data["E17_FORM_SUCCESS"]
                        innerdf["FORM_SUBMISSIONS"] = final_data["FORM_SUBMISSIONS"]
                        innerdf["TOTAL_WEEKLY_UNIQUE_VISITORS"] = final_data["TOTAL_WEEKLY_UNIQUE_VISITORS"]
                        innerdf["ENTRIES"] = final_data["ENTRIES"]
                        innerdf["TOTAL_TIME_SPENT"] = final_data["TOTAL_TIME_SPENT"]
                        innerdf["ACCOUNT_NUMBER"] = pd.Series(final_data["ACCOUNT_NUMBER"])
                        #innerdf.fillna(method='ffill', inplace=True)
                        innerdf["ACCOUNT_NUMBER"]  = innerdf["ACCOUNT_NUMBER"].fillna(method='ffill')
                        innerdf["START_DATE_OF_WEEK"] = pd.Series(final_data["START_DATE_OF_WEEK"])
                        innerdf["START_DATE_OF_WEEK"] = innerdf["START_DATE_OF_WEEK"].fillna(method='ffill')
                        innerdf["GRANULARITY"] = pd.Series(final_data["GRANULARITY"])
                        innerdf["GRANULARITY"] = innerdf["GRANULARITY"].fillna(method='ffill')
                        # outerdf['ETL_EXTRACT_DATE'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        #print (outerdf)
                        #outerdf = pd.concat([outerdf,innerdf], ignore_index=True)
                        outerdf = pd.concat([outerdf, innerdf], axis=0, ignore_index=True)
                        outerdf = outerdf.drop_duplicates()

                columns = ["ITEM", "UNIQUE_VISITORS", "VISITS", "PAGE_VIEWS", "BOUNCES", "TIME_SPENT_ON_PAGE_(MIN)",
                           "E89_VIDEO_VIEWS", "E17_FORM_SUCCESS", "FORM_SUBMISSIONS", "TOTAL_WEEKLY_UNIQUE_VISITORS",
                           "ENTRIES","TOTAL_TIME_SPENT"	, "ACCOUNT_NUMBER", "START_DATE_OF_WEEK", "GRANULARITY", "SEGMENT_ID",
                           "GROUP", "ETL_EXTRACT_DATE"]

                # outerdf['SEGMENT_ID'] = "SUCCESS_PAGE"
                outerdf['SEGMENT_ID'] = "VALUE_CALCULATOR"
                outerdf['GROUP'] = "CUSTOMER_SUCCESS"
                outerdf['ETL_EXTRACT_DATE'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                outerdf["START_DATE_OF_WEEK"] = outerdf["START_DATE_OF_WEEK"].map(self.date_conversion)
                print (outerdf.columns)
                outerdf = outerdf[columns]
                outerdf = outerdf.replace([np.inf, -np.inf], np.nan)
                outerdf = outerdf.replace('', np.NaN)
                outerdf = outerdf.replace('None', np.NaN)
                outerdf = outerdf.replace('nan', np.NaN)
                outerdf = outerdf.where((pd.notnull(outerdf)), None)
                db_obj = PyHdbWrapper()
                cursor, connection = db_obj.connect_hana(
                    Utils.get_file_path(self.da_path, [SCRIPT_FOLDER_NAME, HANA_CONFIG_FILE]), 'HANA_ENV')
                # delete_page_url = db_obj.get_delete_query(self.schema + '.STG_ADOBE_CSC_DAILY')
                # db_obj.execute_sql(cursor, connection, delete_page_url, '', 'DELETE')
                for record in outerdf.to_dict("records"):
                    insert_query = db_obj.get_insert_query(self.schema + ".STG_ADOBE_CSC_WEEKLY",
                                                           record)

                    values = list(record.values())
                    #print(values)
                    print ("Inserting into Staging table")
                    db_obj.execute_sql(cursor, connection, insert_query, tuple(values), 'INSERT')
                    print ("Completed inserting into Staging table")

                print("All records are inserted into Staging table")
                # print ("Upserting into Target table")
                #
                # upsert_statement = "UPSERT \"" + self.schema + "\".\"ADOBE_CSC_DAILY\"  \
                #                                     SELECT * FROM \"" + self.schema + "\".\"STG_ADOBE_CSC_DAILY\""
                # db_obj.execute_sql(cursor, connection, upsert_statement, '', 'UPSERT')
                #
                # print ("Completed upserting to target table")





                #print(outerdf)

                outerdf.to_csv(r'C:\Users\chanukya.konduru\Documents\testing.csv', index=False)

                #     breakdowndf = pd.concat([breakdowndf, pd.DataFrame(element["breakdown"])], ignore_index=True)
                #     # breakdowndf_2 = pd.concat([breakdowndf_2, pd.DataFrame(element["breakdown"][0]["breakdown"])], ignore_index=True)
                #
                #
                #     breakdowndf_2 = pd.DataFrame(element["breakdown"][0]["breakdown"])
                #
                # #print (breakdowndf_2)
                # names = metricsdf['name'].tolist()
                # for i,name in enumerate(names):
                #     breakdowndf[name] = [metricname[i] for metricname in list(breakdowndf['counts'].tolist())]
                #
                # #print (breakdowndf.head())
                # breakdowndf.drop(['counts', 'url'], axis=1, inplace=True)
                # #breakdowndf = breakdowndf[['name', 'e17 Form Success', 'e89 Video Views']]
                # breakdown_length = len(response_body["report"]["data"][0]["breakdown"])
                # breakdowndf["Granularity"] = self.date_granularity
                # #print (breakdowndf)
                # # breakdowndf.to_csv(r'C:\Users\rajkiran.reddy\Desktop\SNow-Projects\Framework\master_\Git_Repositories\adobe_analytics\testing.csv')





                counter_error = counter_error + 1

        #         # Using Pandas library to load json data and transpose it for easy manuplation
        #         adobe_ana_pd = pd.DataFrame.from_dict(response_body)
        #         adobe_ana_pd = adobe_ana_pd.T
        #         # Removing unwanted index from the dataFrame
        #         adobe_ana_pd = adobe_ana_pd.drop(adobe_ana_pd.index[1:])
        #
        #         # The metrics for Adobe Analytics is in 'data' column, so parsing it
        #         final_data_df = pd.DataFrame(adobe_ana_pd['data'][0][0]['breakdown'])
        #         #print (final_data_df.columns)
        #         final_data_df = final_data_df.rename(columns={"counts": "Counts",
        #                                         "name": "Surf ID",
        #                                         "url": "URL"})
        #
        #         final_data_df = final_data_df[["Counts", "Surf ID"]]
        #         final_data_df['Granularity'] = adobe_ana_pd['data'][0][0]['name']
        #
        #         if len(adobe_ana_pd['data'][0][0]['breakdown'][0]['counts']) > 1:
        #             final_data_df['Visits'] = final_data_df['Counts'].map(get_visits)
        #             final_data_df['e17 Form Success'] = final_data_df['Counts'].map(get_form_success)
        #             final_data_df['e89 Video Views'] = final_data_df['Counts'].map(get_video_views)
        #         else:
        #             final_data_df['Return Visits + Visits'] = final_data_df['Counts'].map(get_return_visits)
        #
        #         final_data_df_columns = list(final_data_df.columns)
        #         final_data_df_columns.remove('Counts')
        #         final_data_df = final_data_df[final_data_df_columns]
        #         print (final_data_df)
        #
        #         if 'error' in response_body.keys():
        #             if 'report_not_ready' in response_body['error']:
        #                 pass
        #             else:
        #                 logger.error(api_response.text)
        #                 raise Exception(api_response.reason)
        #         elif 'report' in response_body.keys():
        #             counter_error = 1
            except Exception as e:
                logger.error(e)
                raise
        #
        #
        # # # Using Pandas library to load json data and transpose it for easy manuplation
        # # adobe_ana_pd = pd.DataFrame.from_dict(response_body)
        # # adobe_ana_pd = adobe_ana_pd.T
        # #
        # # # Removing unwanted index from the dataFrame
        # # adobe_ana_pd = adobe_ana_pd.drop(adobe_ana_pd.index[1:])
        # #
        # # # The metrics for Adobe Analytics is in 'data' column, so parsing it
        # # data_df = pd.read_json((adobe_ana_pd['data']).to_json())
        # # #print (data_df.head())
        # #
        # # # datetime.datetime.strptime(str(datetime.date.today()),"%Y-%m-%d")
        # #
        # #
        # # # call delete from table bane before processing response
        # # # also add the migration from staging to main table
        # #
        # # # Iterating over the JSON file to extract metrics
        # # self.response_handler(data_df)
