from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLines(text):
    line = text.split(',')
    custId = int(line[0])
    amt = float(line[2])
    return (custId, amt)

custRDD = sc.textFile("/Users/giridhar.manoharan/Documents/SparkCourse/customer-orders.csv")
cust = custRDD.map(parseLines)
cust = cust.reduceByKey(lambda x,y: x+y)
results = cust.collect()

for custId, amt in results:
    print str(custId)+','+str(amt)
