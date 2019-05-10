import java.io._

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._
import Array._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{array, collect_list}

import scala.collection.mutable



object ying_cheng_task1 {
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


    var node_set = HashSet[Tuple1[String]]()
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
            node_set.add(Tuple1(user_1))
            node_set.add(Tuple1(user_2))
            edge_set.add(Tuple2(user_1, user_2))
//
          }
        }
      }
    }
//    println(node_set)
//    println(edge_set.size)

    var verticels = sqlContext.createDataFrame(node_set.toList)toDF("id")
    var edges = sqlContext.createDataFrame(edge_set.toList).toDF("src", "dst")
    var graph = GraphFrame(verticels, edges)

//    println(graph)
    var result = graph.labelPropagation.maxIter(5).run()
//    println(result.collect())

    var dict_label_userlist = Map[String, Set[String]]()
    for (row<- result.collect()){
//      println(row)
      if (dict_label_userlist.contains(row(1).toString)){
        dict_label_userlist(row(1).toString).add("'"+row(0).toString+"'")
      }
      if (!dict_label_userlist.contains(row(1).toString)){
        dict_label_userlist(row(1).toString) = Set()
        dict_label_userlist(row(1).toString).add("'"+row(0).toString+"'")
      }
//      println(row(0).getClass.getSimpleName)
//      println(row(1).getClass.getSimpleName)
    }
//    println(dict_label_userlist)

    var dict_len_community = Map[Int, ArrayBuffer[List[String]]]()
    for (i<- dict_label_userlist){
      var temp_key = i._2.size
      if(dict_len_community.contains(temp_key)){
        dict_len_community(temp_key).append(i._2.toList.sorted)
      }
      if(!dict_len_community.contains(temp_key)){
        dict_len_community(temp_key) = ArrayBuffer()
        dict_len_community(temp_key).append(i._2.toList.sorted)

      }
//      println(i)
    }

    val writer1 = new PrintWriter(args(2))
    var all_keys = dict_len_community.keys.toList.sorted

//    println(all_keys)
    for (i<- all_keys){
//      println(i)
      var temp_list = dict_len_community(i).sortBy(_(0))
//      temp_list.foreach(print)
//      print(temp_list)
      for(j<- temp_list){
        writer1.write(j.mkString(",").stripSuffix(")").stripPrefix("List("))
        writer1.write("\n")
      }
    }
    writer1.close()




    var end = System.currentTimeMillis()
    println("Duration:" + (end - start) / 1000.0)

  }
}
