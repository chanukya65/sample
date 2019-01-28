import datetime
import pandas as pd
from dateutil.parser import parse
# inputDate1 = "Wed, 28, March, 18"
# DateFormat1 = "%a, %d, %B, %y"
# outPutDateFormat = "%Y/%m/%d"
# date1 = datetime.datetime.strptime(inputDate1 , DateFormat1 )
# print(datetime.date.strftime(date1, outPutDateFormat))

# date_str = '29/12/2017' # The date - 29 Dec 2017
# format_str = '%d/%m/%Y' # The format
# datetime_obj = datetime.datetime.strptime(date_str, format_str)
# print(datetime_obj.date())


# # dt = parse('Mon Feb 15 2010')
# dt = parse('Sat. 6 jan. 2018')
# # print(dt)
# # datetime.datetime(2010, 2, 15, 0, 0)
# print(dt.strftime('%Y-%m-%d'))
# # 15/02/2010

def date_conversion(column_value):
    if (column_value is not None) and (column_value is not '') and (column_value is not pd.np.nan):
        temp_datetime = []
        value = column_value.split(' - ')
        column_value_list_start = list(value[0])
        if column_value_list_start[-6] == '.':
            start_date = datetime.datetime.strptime(value[0], '%b. %Y').strftime('%Y-%m')
            temp_datetime.append(start_date)
        else:
            start_date = datetime.datetime.strptime(value[0], '%b %Y').strftime('%Y-%m')
            temp_datetime.append(start_date)
        return temp_datetime

    else:
        return None


obj = date_conversion("18-Jan")
print("The start date is", obj[0])


# def date_conversion(column_value):
#     if (column_value is not None) and (column_value is not '') and (column_value is not pd.np.nan):
#         value = column_value.split('-')
#         print(value)
#         for i in value:
#
#
# obj = date_conversion("Mon.  1 Jan. 2018 - Sat.  6 Jan. 2018")