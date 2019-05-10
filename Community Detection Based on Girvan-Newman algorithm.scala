import java.io._

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._
import Array._
import scala.collection.mutable._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{array, collect_list}

import scala.collection.mutable



object ying_cheng_task2 {
  def calculate_betweeness(node_set:HashSet[String], dict_node_nodes:Map[String, Set[String]]): Tuple2[Map[(String,String),Float], Map[Float,ArrayBuffer[(String,String)]]] ={
    var dict_edge_betweenness = Map[(String,String),Float]()
    for (each_node<- node_set){
      var dict_layer_node = Map[Float, Set[String]]()
      var layer = 1
      dict_layer_node(layer) = Set[String]()
      dict_layer_node(layer).add(each_node)
      var filter_node_set = Set[String]()
      filter_node_set.add(each_node)
      var next_layer = dict_node_nodes(each_node)

      var dict_node_credit =  Map[String, Float]()
      dict_node_credit(each_node) = 1

      while (next_layer.size!=0){
        layer = layer + 1
        dict_layer_node(layer) = next_layer
        var temp_next_layer = Set[String]()
        for (each_one<- next_layer){
          filter_node_set.add(each_one)
          dict_node_credit(each_one) =1
          for (connect_node<-dict_node_nodes(each_one)){
            temp_next_layer.add(connect_node)
          }
        }
        next_layer = temp_next_layer -- filter_node_set

      }
      var max_range = dict_layer_node.keySet.max.toInt
//      println(dict_layer_node.size)
//      for (index<- range(max_range, 0, -1){
//        println()
//      }
      for (index<- range(max_range, 0, -1)){
      if (index!=1){
        for (each_node<- dict_layer_node(index)){
          var last_layer = dict_node_nodes(each_node) & dict_layer_node(index-1)
          var temp_len = last_layer.size
          var temp_give_credit = dict_node_credit(each_node)/ temp_len
          for(each<- last_layer){
            dict_node_credit(each) = dict_node_credit(each) +temp_give_credit
            var temp_edge = (each, each_node)
            if (each_node<each){
              temp_edge = (each_node, each)

            }
            if (dict_edge_betweenness.contains(temp_edge)){
              dict_edge_betweenness(temp_edge) = dict_edge_betweenness(temp_edge)+temp_give_credit
            }
            if(!dict_edge_betweenness.contains(temp_edge)){
              dict_edge_betweenness(temp_edge) = 0
              dict_edge_betweenness(temp_edge) = dict_edge_betweenness(temp_edge)+temp_give_credit
            }
          }
        }
      }
    }
  }
//    println(dict_edge_betweenness.size)
//    println(dict_edge_betweenness)

  var dict_edge_final_betweenness = Map[(String,String),Float]()
  for (each_key<- dict_edge_betweenness.keySet){
    dict_edge_final_betweenness(each_key) = dict_edge_betweenness(each_key)/2
  }
//    println(dict_edge_final_betweenness)

  var dict_value_edge = Map[Float,ArrayBuffer[(String,String)]]()
  for (i<- dict_edge_final_betweenness.keySet){
    var temp_key = dict_edge_final_betweenness(i)
    if (dict_value_edge.contains(temp_key)){
      dict_value_edge(temp_key).append(i)
    }
    if (!dict_value_edge.contains(temp_key)){
      dict_value_edge(temp_key) = ArrayBuffer()
      dict_value_edge(temp_key).append(i)
    }

  }
//    var dict_edge_final_betweenness = Map[(String,String),Float]()
//    var dict_value_edge = Map[Float,ArrayBuffer[(String,String)]]()


    return Tuple2(dict_edge_final_betweenness, dict_value_edge)
  }


  def calculate_community(node_set:HashSet[String], dict_node_nodes:Map[String, Set[String]]):ArrayBuffer[Set[String]]={
    var community_list = ArrayBuffer[Set[String]]()
    var filter_node_set = Set[String]()
    for(each_node <- node_set) {
      if (!filter_node_set.contains(each_node)) {
        var community = Set[String]()
        community.add(each_node)
        filter_node_set.add(each_node)
        var next_layer = dict_node_nodes(each_node)
        while (next_layer.size != 0) {
          var temp_next_layer = Set[String]()
          for (each_one <- next_layer) {
            filter_node_set.add(each_one)
            community.add(each_one)
            for (connect_node <- dict_node_nodes(each_one)) {
              temp_next_layer.add(connect_node)
            }
          }
          next_layer = temp_next_layer -- filter_node_set
        }
        community_list.append(community)
      }
    }
    return community_list
  }


  def calculate_Q(community_list:ArrayBuffer[Set[String]], dict_node_nodes_original:Map[String, Set[String]], edge_set:HashSet[Tuple2[String, String]], m: Float): Double ={
    var Q = 0.0
    for(each_community<- community_list){
      for(each_node_i<- each_community){
        for(each_node_j<- each_community){
          var temp_edge = (each_node_i, each_node_j)
          if (each_node_j< each_node_i){
            temp_edge = (each_node_j, each_node_i)
          }
          if(edge_set.contains(temp_edge)){
            var temp_score = 1-(dict_node_nodes_original(each_node_i).size)*(dict_node_nodes_original(each_node_j).size)/(2*m)
            Q = Q + temp_score
          }
          if(!edge_set.contains(temp_edge)){
            var temp_score = 0-(dict_node_nodes_original(each_node_i).size)*(dict_node_nodes_original(each_node_j).size)/(2*m)
            Q = Q + temp_score
          }
        }
      }
    }
    return Q/(2*m)
  }


  def main()(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SparkSession.Builder().getOrCreate()

    sc.setLogLevel("ERROR")
    //     sc.setLogLevel("ERROR")
    val start = System.currentTimeMillis()
    //    val reviewFile = sc.textFile("/Users/irischeng/IdeaProjects/Assignment1/yelp_dataset/review.json")
//    val file_path = "/Users/irischeng/INF553/Assignment/hw4/ub_sample_data.csv"
//    val threshold = 7
    val file_path = args(1)
    val threshold = args(0).toInt
    val file = sc.textFile(file_path)
    var header = file.first()
    var file_RDD = file.filter(line => line!= header).map(s => s.split(",")).map(s => (s(0),Set(s(1))))

    var uID_bID_set_RDD = file_RDD.reduceByKey(_++_)
    var all_user_set_RDD =  file_RDD.map(s=> s._1).distinct()

    var dict_uID_bIDset = Map[String, Set[String]]()
    for(i<- uID_bID_set_RDD.collect()){
      dict_uID_bIDset(i._1) = i._2
    }


    var node_set = HashSet[String]()
    var edge_set = HashSet[Tuple2[String, String]]()
    for (user_1<- dict_uID_bIDset.keys){
      for(user_2<- dict_uID_bIDset.keys){
        if (user_1 != user_2){
          var temp_intersection = dict_uID_bIDset(user_1) & dict_uID_bIDset(user_2)
          if(temp_intersection.size >= threshold){
//            var temp_edge = Tuple2(user_1, user_2)
//            if (user_1> user_2){
//              var temp_edge = (user_2, user_1)
//            }
            if (user_1<user_2) {
              var temp_edge = Tuple2("'"+user_1+"'", "'"+user_2+"'")
              edge_set.add(temp_edge)
            }

            if(user_2<user_1){
              var temp_edge = Tuple2("'"+user_2+"'", "'"+user_1+"'")
              edge_set.add(temp_edge)
            }
            node_set.add("'"+user_1+"'")
            node_set.add("'"+user_2+"'")
//            edge_set.add(temp_edge)

            //
          }
        }
      }
    }
//    println(node_set.size)
//    println(edge_set.size)

    var dict_node_nodes = Map[String, Set[String]]()
    for (each_edge<- edge_set){
//      println(each_edge.getClass.getSimpleName)
      if (dict_node_nodes.contains(each_edge._1)){
        dict_node_nodes(each_edge._1).add(each_edge._2)
      }
      if(!dict_node_nodes.contains(each_edge._1)){
        dict_node_nodes(each_edge._1) = Set()
        dict_node_nodes(each_edge._1).add(each_edge._2)
      }
      if (dict_node_nodes.contains(each_edge._2)){
        dict_node_nodes(each_edge._2).add(each_edge._1)
      }
      if(!dict_node_nodes.contains(each_edge._2)){
        dict_node_nodes(each_edge._2) = Set()
        dict_node_nodes(each_edge._2).add(each_edge._1)
      }
    }

    var dict_node_nodes_original = Map[String, Set[String]]()
    for (each_edge<- edge_set){
      //      println(each_edge.getClass.getSimpleName)
      if (dict_node_nodes_original.contains(each_edge._1)){
        dict_node_nodes_original(each_edge._1).add(each_edge._2)
      }
      if(!dict_node_nodes_original.contains(each_edge._1)){
        dict_node_nodes_original(each_edge._1) = Set()
        dict_node_nodes_original(each_edge._1).add(each_edge._2)
      }
      if (dict_node_nodes_original.contains(each_edge._2)){
        dict_node_nodes_original(each_edge._2).add(each_edge._1)
      }
      if(!dict_node_nodes_original.contains(each_edge._2)){
        dict_node_nodes_original(each_edge._2) = Set()
        dict_node_nodes_original(each_edge._2).add(each_edge._1)
      }
    }

    var temp_tuple = calculate_betweeness(node_set,dict_node_nodes_original)
//    println(temp_tuple)
    var dict_edge_betweenness_original = temp_tuple._1
    var dict_value_edge = temp_tuple._2
//    println(dict_value_edge)

    val writer1 = new PrintWriter(args(2))
    for (value<- dict_value_edge.keySet.toList.sorted.reverse){
//      println(value)
      var temp_edge_list = dict_value_edge(value).sorted
      for(each_edge<- temp_edge_list){
        writer1.write(each_edge.toString()+",")
        writer1.write(value.toString)
        writer1.write("\n")
      }
    }
    writer1.close()

    val m =edge_set.size
//    println(m)
    var community_list = calculate_community(node_set,dict_node_nodes)
    var Q_1 = calculate_Q(community_list, dict_node_nodes_original, edge_set, m)
//    println(Q_1)

    var Q_list = ArrayBuffer[Double]()
    Q_list.append(Q_1)

    var all_community_list = ArrayBuffer[ArrayBuffer[Set[String]]]()
    all_community_list.append(community_list)
//    println(community_list)
//    println(community_list.size)

    while (community_list.size< node_set.size){
      var max_betweenness = dict_value_edge.keySet.max
      var cut_edge_list = dict_value_edge(max_betweenness)

      for (each_edge<-cut_edge_list){
        var node_1 = each_edge._1
        var node_2 = each_edge._2
        dict_node_nodes(node_1).remove(node_2)
        dict_node_nodes(node_2).remove(node_1)
      }

      var temp = calculate_betweeness(node_set, dict_node_nodes)
      dict_value_edge = temp._2
      community_list = calculate_community(node_set, dict_node_nodes)
      all_community_list.append(community_list)
      var temp_Q = calculate_Q(community_list, dict_node_nodes_original, edge_set, m)
      Q_list.append(temp_Q)
    }

    var index_max_Q = Q_list.indexOf(Q_list.max)
    var final_community = all_community_list(index_max_Q)

//    println(Q_list.max)
//    println(final_community.size)


    var dict_len_community = Map[Int, ArrayBuffer[List[String]]]()
    for (i<- final_community){
//      println(i)
      var temp_key = i.size
      if(dict_len_community.contains(temp_key)){
        dict_len_community(temp_key).append(i.toList.sorted)
      }
      if(!dict_len_community.contains(temp_key)){
        dict_len_community(temp_key) = ArrayBuffer[List[String]]()
        dict_len_community(temp_key).append(i.toList.sorted)

      }
      //      println(i)
    }

    val writer = new PrintWriter(args(3))
    for(i<-dict_len_community.keySet.toList.sorted){
      var temp_list = dict_len_community(i).sortBy(_(0))
      for(j<- temp_list){
        writer.write(j.mkString(",").stripSuffix(")").stripPrefix("List("))
        writer.write("\n")
      }
    }
    writer.close()




    var end = System.currentTimeMillis()
    println("Duration:" + (end - start) / 1000.0)

  }
}
