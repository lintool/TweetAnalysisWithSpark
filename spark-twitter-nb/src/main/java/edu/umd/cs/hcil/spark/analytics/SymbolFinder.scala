package edu.umd.cs.hcil.spark.analytics


/**
 * Created by cbuntain on 7/10/15.
 */

import twitter4j.json.DataObjectFactory
import twitter4j.Status
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils

object SymbolFinder {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Symbol Finder")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val keywordPath = args(2)

    val keywords = scala.io.Source.fromFile(keywordPath).getLines.toList

    println("Searching for:")
    keywords.foreach(s => println("\t" + s))
    println()

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweets = twitterMsgs.map(line => {
      (line, JsonUtils.jsonToStatus(line))
    })

    val tweetTexts = tweets.filter(tuple => {
      val status = tuple._2

      status != null &&
      status.getText.length > 0
    })

    val relevantTweetJson = tweetTexts.filter(tuple => {
      symbolFinder(keywords, tuple._2)
    }).map(tuple => tuple._1)

    relevantTweetJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def symbolFinder(keywords : List[String], status : Status) : Boolean = {

    val searchText = if ( status.isRetweet == true) {
      status.getText.toLowerCase + status.getRetweetedStatus.getText.toLowerCase
    } else {
      status.getText.toLowerCase
    }

    for ( symbol <- keywords ) {
      if ( searchText.contains(symbol) == true ) {
        return true
      }
    }

    return false
  }
}
