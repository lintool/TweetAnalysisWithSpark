package edu.umd.cs.hcil.spark.analytics

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

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import edu.umd.cs.hcil.spark.analytics.utils.{JsonUtils, TimeScale}
import org.apache.spark.{SparkContext, _}
import org.apache.spark.rdd.RDD
import twitter4j.{Status, TwitterFactory}

object MaximumRetweets {

  /**
   * Print the usage message
   */
  def printHelp() : Unit = {
      println("Usage: spark-submit " + this.getClass.getCanonicalName + "<input_file> <output_dir> [numPartitions]")
  }

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
 
    if ( args.size < 2 ) {
      printHelp()
      sys.exit(1)
    }
    
    val conf = new SparkConf().setAppName("Maximal Retweets")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    
    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)
    
    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 2 ) {
      val initialPartitions = args(2).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size
    
    // Convert each JSON line in the file to a status using Twitter4j
    //  Note that not all lines are Status lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.'
    val retweets = twitterMsgs.map(JsonUtils.jsonToStatus(_)).filter(status => {
        status != null &&
        status.isRetweet &&
        status.getRetweetedStatus.getRetweetCount > 0 // I'm not sure why this is necessary, but for some reason
                                                      //  some retweets' original statuses have zero retweet counts.
      })
    
    val finalRetweets = findMaximalRetweets(retweets)
    val maximalRetweetCounts = finalRetweets.map(status => status.getRetweetedStatus.getRetweetCount)

    def combiner(l : (Long, Long, Long, Long), r : Status) : (Long, Long, Long, Long) = {
      val retweetCount = r.getRetweetedStatus.getRetweetCount.toLong
      (l._1+1, l._2 + retweetCount, java.lang.Math.min(l._3, retweetCount), java.lang.Math.max(l._4, retweetCount))
    }
    def merger(l : (Long, Long, Long, Long), r : (Long, Long, Long, Long)) : (Long, Long, Long, Long) = {
      (l._1+r._1, l._2+r._2, java.lang.Math.min(l._3, r._3), java.lang.Math.max(l._4, r._4))
    }

    val summedCount = finalRetweets.aggregate((0L,0L,Long.MaxValue,0L)) (
      combiner,
      merger)

    val tweetCount = summedCount._1
    val retweetSum = summedCount._2
    val agMin = summedCount._3
    val agMax = summedCount._4
    val meanRetweetCount = retweetSum.toDouble / tweetCount

    println(s"Number of retweets: $tweetCount, Sum of retweets: $retweetSum")
    println(s"Mean Retweet Count: $meanRetweetCount")
    println(s"Min: $agMin, Max: $agMax")

    val sigma = maximalRetweetCounts.stdev()
    println(s"Standard Deviation Count: $sigma")

    val sortedCounts = maximalRetweetCounts.sortBy(i => i)
    val median = sortedCounts.take((tweetCount / 2).toInt).last
    println(s"Median Retweet Count: $median")

    // Extract the JSON for the maximal retweets
    val retweetIds = finalRetweets.map(status =>
      "%s,%d".format(status.getId.toString, status.getRetweetedStatus.getRetweetCount)
    )

    retweetIds.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  /*
   * Given a set of retweets, find the retweet that expresses the _maximum_
   *  number of retweets for the original tweet. We use this value as a proxy
   *  for the maximum number of times a tweet has been retweeted.
   *  Then return the original status referenced in this maximal retweet.
   */
  def findMaximalRetweets(retweets : RDD[Status]) : RDD[Status] = {

    // For every original tweet, get its retweet count and the retweet that
    //  has that count. We will use this to find the original tweet's maximum
    //  retweet count
    val originalTweetIds = retweets.map(retweet => {
      val original = retweet.getRetweetedStatus
      (original.getId, (original.getRetweetCount, retweet))
    })

    // What is the maximum retweet count for each original tweet?
    val maxRetweets = originalTweetIds.reduceByKey((l, r) => {
      val leftCount = l._1
      val leftStatus = l._2
      val rightCount = r._1
      val rightStatus = r._2

      if ( leftCount > rightCount ) {
        (leftCount, leftStatus)
      } else {
        (rightCount, rightStatus)
      }
    })

    // Return the retweet with the max count in the original
    return maxRetweets.map(tup => tup._2._2)
  }

}
