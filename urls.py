from pyspark import SparkContext
from operator import itemgetter
import re
import matplotlib.pyplot as plt

sc = SparkContext('local[*]', 'Top Urls')

input = sc.textFile('/home/lcardiano/code/erro')

print("Lines : %s" % input.count())
labels = [1, 2, 3, 4]
sizes = [38.4, 40.6, 20.7, 10.3]
colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral']
log_regex = '(?P<ip>[(\d\.)]+) - - \[(?P<date>.*?) -(.*?)\] "(?P<method>\w+) (?P<request_path>.*?) HTTP/(?P<http_version>.*?)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'
top_urls = input \
    .map(lambda line: re.match(log_regex, line)) \
    .filter(lambda match: match != None) \
    .map(lambda match: match.groups()[4]) \
    .map(lambda request: request.split()[0]) \
    .countByValue()

for url, count in sorted(top_urls.items(), key=itemgetter(1), reverse=True)[:4]:
    
    labels[url,count] = format(url)
    sizes[url, count] = format(count)
    patches, texts = plt.pie(sizes[x], colors=colors, shadow=True, startangle=90)
    plt.legend(patches, labels[x], loc="best")
    plt.axis('equal')
    plt.tight_layout()
    
    print('{0: >2} => {1: >2}'.format(url, count))

plt.show()
