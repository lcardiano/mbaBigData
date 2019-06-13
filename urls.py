from pyspark import SparkContext
from operator import itemgetter
import re
import matplotlib.pyplot as plt

sc = SparkContext('local[*]', 'Top Urls')

input = sc.textFile('/home/lcardiano/code/www4')

print("Lines : %s" % input.count())
labels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
sizes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', 'blue', 'black', 'yellow', 'pink', 'red', 'orange']
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
    x+=1
    #print('{0: >2} => {1: >2}'.format(url, count))


patches, texts = plt.pie(sizes, colors=colors, shadow=True, startangle=90)
plt.legend(patches, labels, loc="best")  
plt.axis('equal')
plt.tight_layout()    
plt.show()
