package edu.umd.cs.hcil.spark.analytics

/**
  * Created by cbuntain on 7/10/15.
  */

import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import twitter4j.Status

object UserExtractor {

  /**
    * Print the usage message
    */
  def printHelp() : Unit = {
    println("Usage: spark-submit " + this.getClass.getCanonicalName + "<-u|-m> <input_file> <output_dir> [numPartitions]")
    println("\t -u \t Capture just the user")
    println("\t -m \t Capture mentions")
  }

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("User Extractor")
    val sc = new SparkContext(conf)

    val pullType = args(0)
    val dataPath = args(1)
    val outputPath = args(2)
    val userIdPath = args(3)

    // Validate and  pull type
    var mentionFlag = false
    if ( pullType == "-u" ) {
      mentionFlag = false
    } else if ( pullType == "-m" ) {
      mentionFlag = true
    } else {
      printHelp()
      sys.exit(1)
    }

    val userIds : Set[Long] = scala.io.Source.fromFile(userIdPath).getLines.map(s => s.toLong).toSet

    println("Searching for these users:")
    userIds.foreach(s => println("\t" + s))
    println()

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 4 ) {
      val initialPartitions = args(4).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweetPairs : RDD[(Status, String)] = twitterMsgs.map(line => {
      var pair : (Status, String) = null

      try {
        val status = JsonUtils.jsonToStatus(line)

        pair = (status, line)
      } catch {
        case e : Exception => null
      }

      pair
    }).filter(tuple => tuple != null && tuple._1 != null)

    val relevantTweetJson : RDD[String] = if ( mentionFlag == false ) {
      findTweetsByUser(userIds, tweetPairs).map(tup => tup._2)
    } else {
      findTweetsMentioningUser(userIds, tweetPairs).map(tup => tup._2)
    }

    relevantTweetJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def findTweetsByUser(userIds : Set[Long], tweets : RDD[(Status, String)]) : RDD[(Status, String)] = {

    val relevantTweetJson = tweets.filter(tuple => {
      val tweet = tuple._1

      userIds.contains(tweet.getUser.getId)

    })

    return relevantTweetJson
  }

  def findTweetsMentioningUser(userIds : Set[Long], tweets : RDD[(Status, String)]) : RDD[(Status, String)] = {

    val relevantTweetJson = tweets.filter(tuple => {
      val tweet = tuple._1


      val mentionCount = if ( tweet.getUserMentionEntities != null && tweet.getUserMentionEntities.length > 0 ) {
        tweet.getUserMentionEntities.map(entity => if ( userIds.contains(entity.getId) ) { 1 } else { 0 })
          .reduce((l, r) => l+r)
      } else {
        0
      }

      mentionCount > 0 || userIds.contains(tweet.getUser.getId)
    })

    return relevantTweetJson
  }
}

