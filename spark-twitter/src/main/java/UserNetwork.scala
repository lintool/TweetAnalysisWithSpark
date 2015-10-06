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

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import twitter4j.json.DataObjectFactory
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale

object UserNetwork {
  
  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"
  
  // Flag for the scale we are counting in
  object TimeScale extends Enumeration {
    type TimeScale = Value
    val MINUTE, HOURLY, DAILY = Value
  }

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("User Network")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val threshold = args(2).toDouble
    
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
    val tweets = twitterMsgs.map(line => {
        try {
          DataObjectFactory.createStatus(line)
        } catch {
          case e : Exception => null
        }
      })
    
    // Only keep non-null statuses
    val tweetsFiltered = tweets.filter(status => {
        status != null
      })

    val userTuples : RDD[(Long, String)] = tweetsFiltered.flatMap(status => {
      List((status.getUser.getId, status.getUser.getScreenName)) ++ status.getUserMentionEntities.map(entity => (entity.getId, entity.getScreenName))
    })

    val edgeTuples : RDD[Edge[String]] = tweetsFiltered.flatMap(status => {
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
    val subgraph = degreeGraph.subgraph(vpred = (id, attr) => attr._2 > 0)
    val vertexCount = subgraph.vertices.count
    val edgeCount = subgraph.edges.count

    println(s"Vertices: $vertexCount")
    println(s"Edges: $edgeCount")

    val rankedVertices = subgraph.pageRank(0.0001).vertices

    val rankedGraph = subgraph.outerJoinVertices(rankedVertices) { (id, oldAttr, rankOpt) =>
      rankOpt match {
        case Some(rank) => (oldAttr._1, oldAttr._2, rank)
        case None => (oldAttr._1, oldAttr._2, 0d) // No degreeOpt means zero degree
      }
    }

    val rankedVertexCount = rankedGraph.vertices.count
    val rankedEdgeCount = rankedGraph.edges.count

    println(s"Vertices: $rankedVertexCount")
    println(s"Edges: $rankedEdgeCount")

    rankedGraph.vertices.filter(tuple => tuple._2._3 > threshold).sortBy(tuple => tuple._2._3).saveAsTextFile(outputPath)
  }
  
  /**
   * Convert a given date into a string using TIME_FORMAT
   *
   * @param date The date to convert
   */
  def dateFormatter(date : Date) : String = {
    val sdf = new SimpleDateFormat(TIME_FORMAT, Locale.US);
    sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
    
    sdf.format(date)
  }
  
  /**
   * Flatten timestamp to the appropriate scale
   *
   * @param time The date to flatten
   * @param scale The scale we're using
   */
  def convertTimeToSlice(time : Date, scale : TimeScale.Value) : Date = {
    val cal = Calendar.getInstance
    cal.setTime(time)
    
    if ( scale == TimeScale.MINUTE || scale == TimeScale.HOURLY || scale == TimeScale.DAILY ) {
      cal.set(Calendar.SECOND, 0)
    }
    
    if ( scale == TimeScale.HOURLY || scale == TimeScale.DAILY ) {
      cal.set(Calendar.MINUTE, 0)
    }
    
    if ( scale == TimeScale.DAILY ) {
      cal.set(Calendar.HOUR_OF_DAY, 0)
    }
    
    return cal.getTime
  }
  
  /**
   * Build the full date list between start and end
   *
   * @param startDate The first date in the list
   * @param endDate The last date in the list
   * @param scale The scale on which we are counting (minutes, hours, days)
   */
  def constructDateList(startDate : Date, endDate : Date, scale : TimeScale.Value) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)
    
    var l = List[Date]()
    
    while(cal.getTime.before(endDate)) {
      l = l :+ cal.getTime
      
      if ( scale == TimeScale.MINUTE ) {
        cal.add(Calendar.MINUTE, 1)
      } else if ( scale == TimeScale.HOURLY ) {
        cal.add(Calendar.HOUR, 1)
      } else if ( scale == TimeScale.DAILY ) {
        cal.add(Calendar.DATE, 1)
      }
    }
    l = l :+ endDate
    
    return l
  }

}
