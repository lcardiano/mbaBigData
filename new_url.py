#!/usr/bin/python3.6
# -*- coding: UTF-8 -*-

import os
os.environ["SPARK_HOME"] = "/opt/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.6"

import sys
sys.stdout = sys.stderr


from pyspark import SparkContext
from operator import itemgetter
import re
import matplotlib.pyplot as plt

sc = SparkContext('local[*]', 'Top Urls')

input = sc.textFile('/opt/mbaBigData/access_log')

print("Lines : %s" % input.count())
labels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
sizes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', 'blue', 'black', 'yellow', 'pink', 'red', 'orange']

log_regex = '(?P<ip>[(\d\.)]+) - - \[(?P<date>.*?) -(.*?)\] "(?P<method>\w+) (?P<request_path>.*?) HTTP/(?P<http_version>.*?)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'
#log_regex = r'^.* - - \[.*\] "(.*)" \d+ [0-9-]+ ".*" ".*"$'

top_urls = input \
    .map(lambda line: re.match(log_regex, line)) \
    .filter(lambda match: match != None) \
    .map(lambda match: match.groups()[4]) \
    .map(lambda request: request.split()[0]) \
    .countByValue()
x=0
for url, count  in sorted(top_urls.items(), key=itemgetter(1), reverse=True)[:10]:
    labels[x] = format(url)
    labels[x] = labels[x].split('/')[1]
    sizes[x] = format(count)
    labels[x] = labels[x][0:30]
    print('{0: >2} => {1: >2}'.format(url, count))
    x+=1

#for url, count in sorted(top_urls.items(), key=itemgetter(1), reverse=True)[:10]:
#    print('{0: >32} => {1: >8}'.format(url, count))


#patches, texts = plt.pie(sizes, colors=colors, shadow=True, startangle=90)
#plt.legend(patches, labels, loc="best")  
#plt.axis('equal')
#plt.tight_layout()    
#plt.show()
