import os
import sys
from pyspark import SparkContext, SparkConf
# from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time


os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.4.0-spark2.1-s_2.11 pyspark-shell"
)

from graphframes import *


start = time.perf_counter()

sc = SparkContext.getOrCreate()
ss = SparkSession.builder.appName("task1").config("spark.sql.shuffle.partitions",4).getOrCreate()

# file_path ="/Users/irischeng/INF553/Assignment/hw4/ub_sample_data.csv"
file_path = sys.argv[2]
threshold = int(sys.argv[1])
file = sc.textFile(file_path)
header = file.first()
file_RDD = file.filter(lambda row: row != header).map(lambda s:s.split(",")).map(lambda s: (s[0], s[1])).persist()  ##(u,b)
uID_bID_set_RDD = file_RDD.groupByKey().map(lambda row: (row[0], set(row[1])))
all_user_set_RDD = file_RDD.map(lambda s: s[0]).distinct()

dict_uID_bIDset = {}
for i in uID_bID_set_RDD.collect():
    dict_uID_bIDset[i[0]] = i[1]


node_set = set()
edge_set = set()
for user1 in dict_uID_bIDset.keys():
    for user2 in dict_uID_bIDset.keys():
        if user1!=user2:
            # temp = (user1)
            temp_intersection = dict_uID_bIDset[user1] & dict_uID_bIDset[user2]
            if len(temp_intersection) >=threshold:
                temp_edge_1 = (min(user1, user2), max(user1, user2))
                temp_edge_2 = (max(user1, user2), min(user1, user2))
                node_set.add(tuple([user1]))
                node_set.add(tuple([user2]))
                edge_set.add(temp_edge_1)
                edge_set.add(temp_edge_2)
# print(len(edge_set))
# print(len(node_set))


vertices = ss.createDataFrame(node_set, ["id"])
# vertices.show()
edges = ss.createDataFrame(edge_set,["src", "dst"])
# edges.show()
graph = GraphFrame(vertices, edges)
# print(graph)
result = graph.labelPropagation(5)
# result.sort(['label'],ascending=[0]).show()
# result.show()
group = result.groupBy("label").agg(F.collect_list("id")).collect()
# print(group)
# group.sort(['label'],ascending=[0]).show()

## build a dict to store the result, key = len(community) value =[[],[],[]]
dict_len_community = {}
for i in group:
    temp_key = len(i[1])
    if temp_key in dict_len_community:
        dict_len_community[temp_key].append(sorted(i[1]))
    else:
        dict_len_community[temp_key]=[]
        dict_len_community[temp_key].append(sorted(i[1]))

fileObject = open(sys.argv[3], 'w')
for i in sorted(dict_len_community.keys()):
    temp_list = sorted(dict_len_community[i])
    for j in temp_list:
        fileObject.write(str(j).strip("]").strip("["))
        fileObject.write("\n")
fileObject.close()

# print(dict_len_community)
# print(max(dict_len_community.keys()))
end = time.perf_counter()
print("duration:", end-start)






