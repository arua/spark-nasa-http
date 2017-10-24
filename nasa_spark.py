import re
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('nasahttp').setMaster('local')
sc = SparkContext(conf=conf)

def is404(s):
  try:
    return s.split(" ")[-2] == "404"
  except IndexError:
    False

def getTotalBytes(s):
  try:
    return int(s.split(" ")[-1])
  except ValueError:
    return 0

def getDate(s):
  timestamp = re.split(r'\[|\]', s)[1]
  return timestamp.split(":")[0]

rdd = sc.textFile("access_log_*")

num_hosts = rdd.map(lambda s: s.split(" - -")[0]).distinct().count()
print("número de hosts únicos: ", num_hosts)

num_404 = rdd.filter(is404).count()
print("total de erros 404: ", num_404)

d = rdd.filter(is404).map(lambda i: i.split(" ")[0]).countByValue()
hosts_404 = sorted(d, key=lambda x: d[x], reverse=True)[:5]
print("As 5 URLs que mais causaram erros 404: ", hosts_404)

num_404_by_day = rdd.filter(is404).map(getDate).countByValue()
print("quantidade de erros 404 por dia: ", num_404_by_day)

total_bytes = rdd.map(getTotalBytes).reduce(lambda a, b: a+b)
print("total de bytes retornados: ", total_bytes)
