package edu.umd.cs.hcil.spark.analytics

/**
 * Created by cbuntain on 7/10/15.
 */

import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils
import org.apache.spark.{SparkContext, _}
import org.apache.spark.rdd.RDD
import twitter4j.Status

object ThreadBuilder {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Thread Builder")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val topKThreads = args(2).toInt
    val maxDepth = args(3).toInt

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 4 ) {
      val initialPartitions = args(4).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweets = twitterMsgs.map(line => {
      val status = JsonUtils.jsonToStatus(line)
      (status, line)
    })

    // Filter tweets with blank or bad statuses
    val goodTweets = tweets.filter(tup => {
      val status = tup._1

      status != null &&
      status.getText.length > 0
    })
    goodTweets.cache()

    val tweetCount = goodTweets.count()
    println("Tweet Count: %d".format(tweetCount))

    // Extract just the status objects
    val tweetTexts = goodTweets.map(tup => tup._1)

    // Get the map of threads
    val tweetThreads = buildThreads(tweetTexts, topKThreads, maxDepth)

    // For each tweet thread, get the JSON for the thread
    for ( headTweetEntry <- tweetThreads.iterator ) {
      val headTweetId = headTweetEntry._1
      val threadIds = headTweetEntry._2

      println("Root ID: %d, Child Count: %d".format(headTweetId, threadIds.size))

      val threadedJson = goodTweets.filter(tup => {
        val status = tup._1

        threadIds.contains(status.getId)
      }).map(tup => tup._2)

      val threadIdPath = outputPath + "/" + headTweetId.toString
      threadedJson.saveAsTextFile(threadIdPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
    }
  }

  /*
   * Given a set of tweets, find the top K most retweeted tweets, and build threads of conversation
   *  starting with these popular tweets. Return a dictionary that maps the most retweeted tweet IDs
   *  to their descendants.
   */
  def buildThreads(tweets : RDD[Status], topKThreads : Int, maxDepth : Int) : Map[Long, Set[Long]] = {

    // Need to find the top k most retweeted tweets in this set
    val headTweetCounts = tweets
      .filter(status => status.isRetweet)
      .map(retweet => {
        val retweetedStatus = retweet.getRetweetedStatus

        (retweetedStatus.getId, retweetedStatus.getRetweetCount)
      })
      .reduceByKey((l, r) => math.max(l, r))
      .top(topKThreads)

    var threadMap : Map[Long, Set[Long]] = Map()

    // For each of these top K tweets, find descendant tweets (tweets that retweet or respond to this tweet)
    for ( threadHead <- headTweetCounts ) {
      val headStatusId = threadHead._1
      val headStatusCount = threadHead._2

      var levelSets = List(Set(headStatusId))

      // Search for levels of tweets
      for ( i <- 1 until maxDepth ) {
        // Check to make sure we haven't already reached the end of this tree
        //  If levelSets.size == i, then we added a level last time.
        //  Otherwise, we didn't add a level last iteration, and we should stop (stop means skip remaining levels).
        //  This could be solved using "break", but that requires additional syntactic sugar in Scala
        if ( levelSets.size == i ) {
          val previousLevel = levelSets(i - 1)

          // If we have tweets in the previous level, find their children
          if (previousLevel.size > 0) {

            // Get the IDs of all tweets that reply to a tweet in the previous level
            val thisLevelResponses = tweets
              .filter(status => {
                if ( status.getInReplyToStatusId != null ) {
                  previousLevel.contains(status.getInReplyToStatusId)
                } else {
                  false
                }
              })
              .map(status => status.getId)
              .collect()
              .toSet

            levelSets = levelSets :+ thisLevelResponses
          }
        }
      }

      // levelSets now contains a tree-like structure of tweets that respond to the original head tweet
      //  in some way. Tweet IDs in level i are the grand*(i-1) children of the head.
      val tweetIdSet = levelSets.reduce((l, r) => l ++ r)
      threadMap = threadMap ++ Map(headStatusId -> tweetIdSet)
    }

    return threadMap
  }

}
