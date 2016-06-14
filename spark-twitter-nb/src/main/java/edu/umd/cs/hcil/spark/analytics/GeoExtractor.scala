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

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import twitter4j.json.DataObjectFactory

object GeoExtractor {

  /**
   * Filter out tweets without attached GPS coordinates
   *
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GeoExtractor")
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
    val tweets = twitterMsgs.map(line => {
        try {
          (line, DataObjectFactory.createStatus(line))
        } catch {
          case e : Exception => (line, null)
        }
      })
    
    // Only keep non-null status with text
    val tweetsFiltered = tweets.filter(tuple => {
        val status = tuple._2
        
        status != null &&
        status.getText != null &&
        status.getText.size > 0
      })
    
    // Filter again to keep tweets with a geo feature
    val geoTweets = tweetsFiltered.filter(tuple => {
        val status = tuple._2
        
        status.getGeoLocation != null
      })
    
    // Map tuple of (json, tweet) to (date, json), so we can organize them
    val datedGeoTweets = geoTweets.map(tuple => {
        (tuple._2.getCreatedAt, tuple._1)
      })
    
    datedGeoTweets.repartition(newPartitionSize).sortByKey()
      .map(tuple => tuple._2)
      .saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
