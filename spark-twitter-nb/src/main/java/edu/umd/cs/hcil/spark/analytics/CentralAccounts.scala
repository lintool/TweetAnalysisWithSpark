/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cs.hcil.spark.analytics

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import twitter4j.json.DataObjectFactory
import twitter4j.Status
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale

import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils

object CentralAccounts {

  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("User Network")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val threshold = args(2).toDouble
    val tolerance = args(3).toDouble

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size

    // Convert each JSON line in the file to a status using Twitter4j
    //  Note that not all lines are Status lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.'
    val tweets = twitterMsgs.map(JsonUtils.jsonToStatus(_))

    // Only keep non-null statuses
    val tweetsFiltered = tweets.filter(status => {
      status != null
    })

    val subgraph = getGraph(tweetsFiltered)

    val vertexCount = subgraph.vertices.count
    val edgeCount = subgraph.edges.count

    println(s"Vertices: $vertexCount")
    println(s"Edges: $edgeCount")

    val rankedNodes = rankNodes(subgraph, threshold, tolerance)
    val tmpList = rankedNodes.take(10)

    rankedNodes.saveAsTextFile(outputPath)
  }

  def getGraph(tweets : RDD[Status]) : Graph[(String, Int), String] = {
    val userTuples : RDD[(Long, String)] = tweets.flatMap(status => {
      List((status.getUser.getId, status.getUser.getScreenName)) ++ status.getUserMentionEntities.map(entity => (entity.getId, entity.getScreenName))
    })

    val edgeTuples : RDD[Edge[String]] = tweets.flatMap(status => {
      val userId = status.getUser.getId

      status.getUserMentionEntities.map(entity => Edge(userId, entity.getId, "mentions"))
    })

    val graph = Graph(userTuples, edgeTuples)
    val degreeGraph = graph.outerJoinVertices(graph.inDegrees) { (id, oldAttr, degreeOpt) =>
      degreeOpt match {
        case Some(degree) => (oldAttr, degree)
        case None => (oldAttr, 0) // No degreeOpt means zero degree
      }
    }

    return degreeGraph.subgraph(vpred = (id, attr) => attr._2 > 0)
  }

  def rankNodes(graph : Graph[(String, Int), String]) : RDD[(graphx.VertexId, (String, Int, Double))] = {
    rankNodes(graph, 0.0d, 0.001)
  }

  def rankNodes(graph : Graph[(String, Int), String], threshold : Double, tolerance : Double) : RDD[(graphx.VertexId, (String, Int, Double))] = {

    val rankedVertices = graph.pageRank(tolerance).vertices

    val rankedGraph = graph.outerJoinVertices(rankedVertices) { (id, oldAttr, rankOpt) =>
      rankOpt match {
        case Some(rank) => (oldAttr._1, oldAttr._2, rank)
        case None => (oldAttr._1, oldAttr._2, 0d) // No degreeOpt means zero degree
      }
    }

    return rankedGraph.vertices.filter(tuple => tuple._2._3 > threshold) //.sortBy(tuple => tuple._2._3)
  }
}
