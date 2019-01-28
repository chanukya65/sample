import argparse

#from adobe.adobe_base import AdobeBase
from adobe_base import AdobeBase
from base.utils import Utils

p = argparse.ArgumentParser()
# p.add_argument('da-path')
# p.add_argument('date-from')
# p.add_argument('date-to')
# p.add_argument('date-granularity')
# p.add_argument('sleep-time')
# p.add_argument('schema-name')
# p.add_argument('table-name')

# p.add_argument('metric-list')
# p.add_argument('element-list')
# p.add_argument('element-names')
# p.add_argument('segment-list')
p.add_argument('date-from')
p.add_argument('date-to')
p.add_argument('date-granularity')
p.add_argument('sleep-time')
p.add_argument('schema-name')
p.add_argument('segment-name')
p.add_argument('da-path')



if __name__ == "__main__":
    args = vars(p.parse_args())
    #Utils.set_error_logging(Utils.get_file_path(args['da-path'], ['AdobeAnalytics', 'LOGS']), "adobe_analytics")
    #print (args)
    adobe_base_object = AdobeBase(args)
    adobe_base_object.main()
