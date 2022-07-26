import time
import sys
import math
from bloomfilter import Bloomfilter
from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP

def splitter(line):
    items = line.split("\t")
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))
def get_m_k(n, p):
    m = int(round((-n * math.log(p)) / (math.log(2) ** 2)))
    k = int(round(m * math.log(2) / n))
    return m, k
def bloomfilter_test(inn):
    counter = []
    (film_id, rate) = splitter(inn)
    for bf in broadcast_bf.value:
        if rate != bf[0]:
            result = bf[1].find(film_id)
            if result is True:
                counter.append(tuple((bf[0], 1)))
    return counter
def add_in_bloomfilters(inn):
    bloomfilters = [Bloomfilter(m, k) for m, k in broadcast_params.value]
    for line in inn:
        (id, rate) = splitter(line)
        bloomfilters[rate - 1].add(id)
    rate = range(1, 11)
    return zip(rate, bloomfilters)

if __name__ == "__main__":
    IN_PATH = 'data.tsv'
    OUT_PATH = 'output'
    p = 0.01
    sc = SparkContext(appName="BloomFilter", master="yarn")
    sc.setLogLevel("WARN")
    sc.addPyFile("bloomfilter.zip")
    rdd_input = sc.textFile(IN_PATH).cache()
    start_time = time.time()

    rdd_films = rdd_input.map(splitter)
    rdd_counts_by_rating = rdd_films.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).sortByKey()
    counts_by_rating = rdd_counts_by_rating.collect()
    params = [get_m_k(n, p) for rating, n in counts_by_rating]
    broadcast_params = sc.broadcast(params)
    rdd_partial_bf = rdd_input.mapPartitions(add_in_bloomfilters)
    rdd_final_bf = rdd_partial_bf.reduceByKey(lambda filter1, filter2: filter1.bitwise_or(filter2)).sortByKey()
    broadcast_bf = sc.broadcast(rdd_final_bf.collect())
    rdd_counter = rdd_input.flatMap(bloomfilter_test)
    rdd_false_positive_count = rdd_counter.reduceByKey(lambda x, y: x + y).sortByKey()
    false_positive_count = rdd_false_positive_count.collect()
    counts_by_rating = rdd_counts_by_rating.collect()
    film_count = rdd_input.count()
    with open(OUT_PATH, "w") as f:
        for i in range(10):
            false_positive_rate = false_positive_count[i][1]/(film_count - counts_by_rating[i][1])
            f.write(str(i+1) + "\t" + str(false_positive_rate) + "\n")
    passed = time.time() - start_time
    print("time passed:", passed)
