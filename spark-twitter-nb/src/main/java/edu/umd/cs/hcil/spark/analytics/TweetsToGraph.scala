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

import java.io.{File, FileWriter, Writer}

import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils
import it.uniroma1.dis.wsngroup.gexf4j.core.data.{AttributeClass, AttributeType}
import it.uniroma1.dis.wsngroup.gexf4j.core.{EdgeType, Gexf, Mode, Node}
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.{GexfImpl, StaxGraphWriter}
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl
import org.apache.spark.{SparkContext, _}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import twitter4j.Status

object TweetsToGraph {

  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"

  case class TwitterUser(id : Long, name : String)

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("User Network")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

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

    // Build the distributed graph and the collect the users and edges
    val graph = getGraph(tweetsFiltered)
    val userMap = graph.vertices.collectAsMap()
    val edges = graph.edges.collect()

    // Construct the gexf graph
    val gexf = new GexfImpl()

    val gexfGraph = gexf.getGraph()
    gexfGraph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.STATIC)

    val nodeAttributes = new AttributeListImpl(AttributeClass.NODE)
    val nodeAttrId = nodeAttributes.createAttribute("0", AttributeType.LONG, "userId")
    val nodeAttrName = nodeAttributes.createAttribute("1", AttributeType.STRING, "screenname")
    gexfGraph.getAttributeLists().add(nodeAttributes)

    // Add all the nodes
    var nodeMap = Map[Long,Node]()
    for ( userTuple <- userMap.toList ) {
      val nodeId = userTuple._1
      val user = userTuple._2

      val node = gexfGraph.createNode(nodeId.toString)
      node.setLabel(user.name)
        .getAttributeValues
        .addValue(nodeAttrId, user.id.toString)
        .addValue(nodeAttrName, user.name)

      nodeMap = nodeMap + (nodeId -> node)
    }

    // Add the edges
    for ( edge <- edges ) {
      val srcId = edge.srcId.toLong
      val dstId = edge.dstId.toLong
      val weight = edge.attr

      val srcNode = nodeMap(srcId)
      val dstNode = nodeMap(dstId)

      val srcName = srcNode.getAttributeValues.get(1).getValue
      val dstName = dstNode.getAttributeValues.get(1).getValue

      val newEdge = srcNode.connectTo(edge.hashCode().toString, "mentions", EdgeType.DIRECTED, dstNode)
      newEdge.setWeight(weight)
    }

    // Write the file to disk
    try {
      val graphWriter = new StaxGraphWriter()
      val outFile = new File(outputPath)
      val fileWriter = new FileWriter(outFile, false)

      graphWriter.writeToStream(gexf, fileWriter, "UTF-8")
    } catch {
      case e : Throwable => println("Caught exception:" + e.getMessage)
    }
  }

  def getGraph(tweets : RDD[Status]) : Graph[TwitterUser, Int] = {

    val userMentionMap = tweets.flatMap(status => {
      val author = TwitterUser(status.getUser.getId, status.getUser.getScreenName)

      status.getUserMentionEntities.map(entity => {
        val mentionedUser = TwitterUser(entity.getId, entity.getScreenName)
        ("%d,%d".format(author.id, mentionedUser.id), (author, mentionedUser, 1))
      })
    }).reduceByKey((l, r) => {
      val src = l._1
      val dst = l._2

      (src, dst, l._3 + r._3)
    })

    val users = userMentionMap.values.flatMap(tuple => {
      val src : TwitterUser = tuple._1
      val dst : TwitterUser = tuple._2
      Array((src.id, src), (dst.id, dst))
    })

    val edges : RDD[Edge[Int]] = userMentionMap.values.map(tuple => {
      val src = tuple._1
      val dst = tuple._2
      val weight : Int = tuple._3

      Edge(src.id, dst.id, weight)
    })

    val graph = Graph(users, edges)
    val degreeGraph = graph.outerJoinVertices(graph.degrees) { (id, oldAttr, degreeOpt) =>
      degreeOpt match {
        case Some(degree) => (oldAttr, degree)
        case None => (oldAttr, 0) // No degreeOpt means zero degree
      }
    }

    val subgraphWithDegree = degreeGraph.subgraph(vpred = (id, attr) => attr._2 > 0)
    val trimmedSub = subgraphWithDegree.mapVertices((id, attr) => attr._1)

    return trimmedSub
  }
}
