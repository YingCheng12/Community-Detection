from pyspark import SparkContext
import itertools
import os
import time
import sys
import copy



def calculate_betweeness(node_set, dict_node_nodes):

    dict_edge_betweenness = {}
    for each_node in node_set:
        dict_layer_node = {}
        layer = 1
        dict_layer_node[layer] = set([each_node])
        # print(dict_layer_node)

        filter_node_set = set([each_node])
        next_layer = dict_node_nodes[each_node]

        dict_node_credit = {}
        dict_node_credit[each_node] = 1

        while (len(next_layer) != 0):
            layer = layer + 1
            # print(layer)
            dict_layer_node[layer] = next_layer  # dict_layer [2] = 2,3
            # print(dict_layer_node)
            temp_next_layer = set()
            for each_one in next_layer:
                # print(each_one)
                filter_node_set.add(each_one)
                # print(filter_node_set)
                dict_node_credit[each_one] =1
                for connect_node in dict_node_nodes[each_one]:
                    # print(connect_node)
                    temp_next_layer.add(connect_node)
                # print(temp_next_layer)
                # print()

            next_layer = temp_next_layer - filter_node_set

            # print(next_layer)
            # print()
        # print(dict_node_credit)
        # print(len(dict_layer_node))

        # max = max(dict_layer_node.keys())
        # print(max)
        for index in range(max(dict_layer_node.keys()), 0, -1):
            # print (index)
            if index != 1:
                for each_node in dict_layer_node[index]:
                    # print(each_node)
                    last_layer = dict_node_nodes[each_node] & dict_layer_node[index-1]
                    # print(last_layer)
                    temp_len = len(last_layer)
                    temp_give_credit = dict_node_credit[each_node] / temp_len
                    # print(temp_give_credit)
                    for each in last_layer:
                        dict_node_credit[each] = dict_node_credit[each] + temp_give_credit
                        # print(each)
                        temp_edge = (min(each_node, each), max(each_node,each))
                        if temp_edge in dict_edge_betweenness:
                            dict_edge_betweenness[temp_edge] = dict_edge_betweenness[temp_edge]+temp_give_credit
                        else:
                            dict_edge_betweenness[temp_edge] = 0
                            dict_edge_betweenness[temp_edge] = dict_edge_betweenness[temp_edge]+temp_give_credit
    # print(len(dict_edge_betweenness))
    dict_edge_final_betweenness = {}
    for each_key in dict_edge_betweenness:
        dict_edge_final_betweenness[each_key] = dict_edge_betweenness[each_key] / 2

    dict_value_edge = {}  # {2.5: [(2, 4), (1, 2)]}
    for i in dict_edge_final_betweenness.keys():
        temp_key = dict_edge_final_betweenness[i]  # value 345
        if temp_key in dict_value_edge:
            dict_value_edge[temp_key].append(i)
        else:
            dict_value_edge[temp_key] = []
            dict_value_edge[temp_key].append(i)

    # print(dict_edge_final_betweenness)
    return dict_edge_final_betweenness, dict_value_edge


def calculate_community(node_set, dict_node_nodes):
    community_list = []
    filter_node_set = set()
    for each_node in node_set:
        if each_node not in filter_node_set:
            community = set()
            community.add(each_node)
            filter_node_set.add(each_node)
            next_layer = dict_node_nodes[each_node]
            while (len(next_layer) != 0):
                temp_next_layer = set()
                for each_one in next_layer:
                    filter_node_set.add(each_one)
                    community.add(each_one)
                    for connect_node in dict_node_nodes[each_one]:
                        temp_next_layer.add(connect_node)
                next_layer = temp_next_layer - filter_node_set

            community_list.append(community)
    return community_list



def calculate_Q(community_list, dict_node_nodes_original, edge_set, m):
    Q = 0
    for each_community in community_list:
        for each_node_i in each_community:
            for each_node_j in each_community:
                temp_edge = (each_node_i, each_node_j)
                if each_node_j<each_node_i:
                    temp_edge = (each_node_j, each_node_i)
                if temp_edge in edge_set:
                # if each_node_i in dict_node_nodes_original[each_node_j]:
                    # print("-")
                    temp_score = 1- len(dict_node_nodes_original[each_node_i])*len(dict_node_nodes_original[each_node_j])/(2*m)
                    Q = Q + temp_score
                else:
                    # print("+")
                    temp_score = 0- len(dict_node_nodes_original[each_node_i])*len(dict_node_nodes_original[each_node_j])/(2*m)
                    Q = Q + temp_score
    return Q/(2*m)






start = time.perf_counter()

sc = SparkContext('local[*]', 'Task2')

# file_path ="/Users/irischeng/INF553/Assignment/hw4/ub_sample_data.csv"
# threshold = 7
threshold = int(sys.argv[1])
file_path = sys.argv[2]
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
                temp_edge = (min(user1, user2), max(user1,user2))
                node_set.add(user1)
                node_set.add(user2)
                edge_set.add(temp_edge)
# print(len(edge_set))
# print(len(node_set))
# dict_node_nodes_original ={}
dict_node_nodes = {}      ## 'HLY9oDcVBH9D25lU4X_V5Q': {'LaiylSIbrA3aPvOYtl-J4A', 'wXdrUQg4-VkSZH1FG4Byzw'}
for each_edge in edge_set:
    if each_edge[0] in dict_node_nodes:
        # print(dict_node_nodes[each_edge[0]])
        dict_node_nodes[each_edge[0]].add(each_edge[1])
    else:
        dict_node_nodes[each_edge[0]] = set()
        dict_node_nodes[each_edge[0]].add(each_edge[1])
    if each_edge[1] in dict_node_nodes:
        # print(dict_node_nodes[each_edge[1]])
        dict_node_nodes[each_edge[1]].add(each_edge[0])
    else:
        dict_node_nodes[each_edge[1]] = set()
        dict_node_nodes[each_edge[1]].add(each_edge[0])

# dict_node_nodes_original = {}
# for each_edge in edge_set:
#     if each_edge[0] in dict_node_nodes_original:
#         # print(dict_node_nodes[each_edge[0]])
#         dict_node_nodes_original[each_edge[0]].add(each_edge[1])
#     else:
#         dict_node_nodes_original[each_edge[0]] = set()
#         dict_node_nodes_original[each_edge[0]].add(each_edge[1])
#     if each_edge[1] in dict_node_nodes_original:
#         # print(dict_node_nodes[each_edge[1]])
#         dict_node_nodes_original[each_edge[1]].add(each_edge[0])
#     else:
#         dict_node_nodes_original[each_edge[1]] = set()
#         dict_node_nodes_original[each_edge[1]].add(each_edge[0])


dict_node_nodes_original = copy.deepcopy(dict_node_nodes)

# print(len(dict_node_nodes.keys()))
# if dict_node_nodes.keys()==node_set:
#     print("true")

dict_edge_betweenness_original, dict_value_edge = calculate_betweeness(node_set, dict_node_nodes_original)


fileObject = open(sys.argv[3], 'w')
for value in sorted(dict_value_edge.keys(),reverse = True):
    # print(value)
    temp_edge_list = sorted(dict_value_edge[value])
    # print(dict_value_edge[value])
    for each_edge in temp_edge_list:
        fileObject.write(str(each_edge)+",")
        fileObject.write(str(value))
        fileObject.write("\n")
fileObject.close()



m = len(edge_set)
### identify community
community_list = calculate_community(node_set, dict_node_nodes)
# print(len(community_list))
Q_1 = calculate_Q(community_list, dict_node_nodes_original, edge_set, m)
# print(Q_1)

Q_list = []
Q_list.append(Q_1)

all_community_list = []
all_community_list.append(community_list)




# dict_node_nodes[]

while len(community_list) < len(node_set):
# while len(dict_value_edge.keys())!=0:
    max_betweenness = max(dict_value_edge.keys())
    # print(max_betweenness)
    cut_edge_list = dict_value_edge[max_betweenness]
    # print(cut_edge_list)
    # print(len(cut_edge_list))

    # print(len(dict_node_nodes["cyuDrrG5eEK-TZI867MUPA"]))

    for each_edge in cut_edge_list:
        # print(each_edge)
        node_1 = each_edge[0]
        node_2 = each_edge[1]
        # print(node_1)
        # print(node_2)
        # print(len(dict_node_nodes[node_1]))
        dict_node_nodes[node_1].remove(node_2)
        dict_node_nodes[node_2].remove(node_1)
        # print(len(dict_node_nodes[node_1]))

    # print(dict_node_nodes)

    dict_edge_betweenness, dict_value_edge = calculate_betweeness(node_set, dict_node_nodes)
    # dict_value_edge_list.append(dict_value_edge)
    community_list = calculate_community(node_set, dict_node_nodes)
    # print(len(community_list))
    all_community_list.append(community_list)
    # print(len(community_list))
    temp_Q = calculate_Q(community_list, dict_node_nodes_original, edge_set, m)
    # print(temp_Q)
    Q_list.append(temp_Q)

# print("+")
# print(Q_list[0])
# print(Q_list)
# print(len(Q_list))
# print(max(Q_list))
index_max_Q = Q_list.index(max(Q_list))

final_community = all_community_list[index_max_Q]
# print(len(final_community))



dict_len_community = {}
for i in final_community:
    # print(i)
    temp_key = len(i)
    if temp_key in dict_len_community:
        dict_len_community[temp_key].append(sorted(i))
    else:
        dict_len_community[temp_key]=[]
        dict_len_community[temp_key].append(sorted(i))

# print(dict_len_community)

fileObject = open(sys.argv[4], 'w')
for i in sorted(dict_len_community.keys()):
    temp_list = sorted(dict_len_community[i])
    for j in temp_list:
        fileObject.write(str(j).strip("]").strip("["))
        fileObject.write("\n")
fileObject.close()


end = time.perf_counter()
print("duration:", end-start)














