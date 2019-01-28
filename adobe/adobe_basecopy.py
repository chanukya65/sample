import base64
import datetime
import hashlib
import json
import logging

import pandas as pd
import random
import time

import pytz
from pandas.io.json import json_normalize

from adobe.constants import API_HOST, PATH, CONFIG_FILE, HANA_CONFIG_FILE, SCRIPT_FOLDER_NAME, MODULE_FOLDER_NAME, \
    LOGGER_NAME
from base.base_api_interface import BaseApiInterface
from base.db_wrapper import PyHdbWrapper
from base.utils import Utils

nonce_seed = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c',
              'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
              'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C',
              'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
              'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

logger = logging.getLogger(__name__)


# AdobeCONNECT

class AdobeBase(BaseApiInterface):
    def __init__(self, kargs):
        logger.info("class initiated")
        self.da_path = kargs['da-path']
        self.date_from = kargs['date-from']
        self.date_to = kargs['date-to']
        self.date_granularity = kargs['date-granularity']
        self.sleep_time = float(kargs['sleep-time'])
        # self.logger = Utils.set_error_logging(
        #     Utils.get_file_path(self.da_path, MODULE_FOLDER_NAME, 'ERROR_LOG'),
        #     LOGGER_NAME)

    #
    def get_endpoint_url(self, query):
        """
        Function is used to generate endpoint url.
        :param query: Query parameters
        :return:
        """
        return API_HOST + PATH + query

    # This Function is used to generate payload which will send as an body in the api call.
    def get_payload(self, dateFrom, dateTo, dateGranularity):
        """
        Function is used to get the payload.
        :param dateFrom: From date or start date
        :param dateTo:  To date or end date
        :param dateGranularity: day, month, year
        :return:
        """
        return {
            'reportDescription': {
                'reportSuiteID': 'servn-geneva-prod',  # servn-servicenow.com-prod
                'dateFrom': dateFrom,
                'dateTo': dateTo,
                'dateGranularity': dateGranularity,
                'locale': 'en_US',
                'metrics': [
                    {
                        'id': 'pageviews'
                    },
                    {
                        'id': 'visits'
                    },
                    {
                        'id': 'uniquevisitors'
                    },
                    {
                        'id': 'bouncerate'
                    },
                    {
                        'id': 'totalTimeSpent'
                    }
                ],
                'elements': [
                    {
                        'id': 'category'
                    },
                    {
                        'id': 'eVar2',
                        'top': 1000,
                    }
                ]
            }
        }
# This is preparing header which contain the authtication information
    def get_header(self, username, digest, nonce_b, iso_time):
        """
        This is preparing header which contain the authtication information

        :param username: username
        :param digest: digest
        :param nonce_b: nonce_b
        :param iso_time:
        :return:
        """
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
        """
         This is used to create a digest
        :param api_secret:
        :return:
        """
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
        """

       This is used for Parsing JSON data and saving it into HANA DB #########

        :param data_df: it include json data and loading json data in hana database
        """
        db_obj = PyHdbWrapper()
        cursor, connection = db_obj.connect_hana(
            Utils.get_file_path(self.da_path, [SCRIPT_FOLDER_NAME, HANA_CONFIG_FILE]), 'HANA_ENV')

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
                        insert_query = db_obj.get_insert_query("SAMEER_RATHOD.STG_PAGE_URL_METRICS", column_name)

                        values = [source_date, self.date_granularity, country, url, pageviews, visits,
                                  uniquevisitors, bouncerate, averageTimeSpentOnSite, extract_date]

                        db_obj.execute_sql(cursor, connection, insert_query, values, 'INSERT')

    def main(self):
        """
            calling utils() Method
               Make connection to database by reading connection parameters from an ini file.
               """
        utils_object = Utils()
        """
           from_ini= It will make the database connection.
           get_file_path= it will Read the file path of Adobe_Analytics and return us a "username " and " api_secret"
        """
        adobe_config = utils_object.from_ini(Utils.get_file_path(self.da_path, [SCRIPT_FOLDER_NAME, CONFIG_FILE]),
                                             'Adobe_Analytics',
                                             ('username', 'api_secret'))

        """ 
        get_endpoint_url= calling get_endpoint_url to generate endpoint url
        """
        query_url = self.get_endpoint_url('method=Report.Queue')

        """ 
                get_payload= calling get_payload to get payload which we will use in json body 
        """
        payload = json.dumps(self.get_payload(self.date_from, self.date_to, self.date_granularity))

        nonce_b, iso_time, digest = self.get_unique_connection_parameters(adobe_config['api_secret'])

        head = self.get_header(adobe_config['username'], digest, nonce_b, iso_time)
        """
         Api Method 'POST' = It will send the request 
        """
        report_queue_api_response = utils_object.send_request('POST', query_url, payload, head)

        report_queue_response_body = report_queue_api_response.text.encode('ascii')
        """
        After API POST Request we are encoding the response in ascii format
        """
        temp_var = report_queue_response_body.split(b':')
        report_id = temp_var[1].replace(b'}', b'')
        print(report_id)

        ######## Section - 2: Get data based on report developed and save the JSON reply in shared folder ########
        # Develoiping API URL for retriving
        query_url = self.get_endpoint_url('method=Report.Get')

        # The body of the API url is enlcosed as post_params
        bodydata = {
            'reportID': '' + report_id.decode('ascii') + ''
        }
        payload = json.dumps(bodydata)

        counter_error = 0
        while (counter_error == 0):

            """"
             While Loop is used to check the api response if any error is there loop will pass else break.            
            """
            # Using sleep method to give enough time to get the report ready to pull the data else it will throw
            # "Report not ready"
            print("Start sleep time " + time.strftime("%X"))
            time.sleep(self.sleep_time)

            nonce_b, iso_time, digest = self.get_unique_connection_parameters(adobe_config['api_secret'])
            head = self.get_header(adobe_config['username'], digest, nonce_b, iso_time)
            # logger
            api_response = utils_object.send_request('POST', query_url, payload, head)
            response_body = json.loads(api_response.text)
            if 'error' in response_body.keys():
                if 'report_not_ready' in response_body['error']:
                    pass
                else:
                    break
            elif 'report' in response_body.keys():
                counter_error = 1

        # Using Pandas library to load json data and transpose it for easy manuplation
        adobe_ana_pd = pd.DataFrame.from_dict(response_body)
        adobe_ana_pd = adobe_ana_pd.T

        # Removing unwanted index from the dataFrame
        adobe_ana_pd = adobe_ana_pd.drop(adobe_ana_pd.index[1:])

        # The metrics for Adobe Analytics is in 'data' column, so parsing it
        data_df = pd.read_json((adobe_ana_pd['data']).to_json())

        # datetime.datetime.strptime(str(datetime.date.today()),"%Y-%m-%d")

        # Iterating over the JSON file to extract metrics
        self.response_handler(data_df)
