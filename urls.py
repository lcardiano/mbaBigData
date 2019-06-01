    
from pyspark import SparkContext
from operator import itemgetter
import re

sc = SparkContext('local[*]', 'Top Urls')

input = sc.textFile('log')

print("Lines : %s" % input.count())


log_regex = r'^.* - - \[.*\] "(.*)" \d+ [0-9-]+ ".*" ".*"$'
top_urls = input \
    .map(lambda line: re.match(log_regex, line)) \
    .filter(lambda match: match != None) \
    .map(lambda match: match.groups()[0]) \
    .map(lambda request: request.split()[1]) \
    .countByValue()

for url, count in sorted(top_urls.items(), key=itemgetter(1), reverse=True)[:10]:
    print('{0: >32} => {1: >8}'.format(url, count))
