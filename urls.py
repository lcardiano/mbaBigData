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

sc = SparkContext('local[*]', 'Top Urls')

input = sc.textFile('/opt/mbaBigData/access_log')

print("Lines : %s" % input.count())
labels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
sizes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


log_regex = '(?P<ip>[(\d\.)]+) - - \[(?P<date>.*?) -(.*?)\] "(?P<method>\w+) (?P<request_path>.*?) HTTP/(?P<http_version>.*?)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'

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
    if labels[x] != 'favicon.ico':
      if labels[x] !=  '':
        print('{0: >2} => {1: >2}'.format(url, count))
        x+=1

