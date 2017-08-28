package edu.umd.cs.hcil.spark.analytics


/**
 * Created by cbuntain on 7/10/15.
 */

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.lang.reflect.Type

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import edu.umd.cs.hcil.spark.analytics.utils.{JsonUtils, StatusTokenizer}
import twitter4j.Status

object KeywordFinder {

  def gson = { new Gson() }
  val mapType : Type = new TypeToken[java.util.HashMap[String,Object]]() {}.getType()

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KeywordFinder")
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

    val tweets = twitterMsgs.map(line => (line, JsonUtils.jsonToStatus(line))).filter(item => item._2 != null)
    val tweetTexts = tweets.filter(tuple => {
      val status = tuple._2

      status.getText.length > 0
    })

    val relevantTweetJson = tweetTexts.filter(tuple => {
      try {
        keywordFilter(keywords, tuple._2)
      } catch {
        case e: Exception =>
          println(tuple._1)
          throw e
      }
    }).map(tuple => tuple._1)

    relevantTweetJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def keywordFilter(keywords : List[String], status : Status) : Boolean = {
    val tokens = StatusTokenizer.tokenize(status).map(str => str.toLowerCase)
    val intersection = tokens.intersect(keywords)

    return intersection.length > 0
  }
}
