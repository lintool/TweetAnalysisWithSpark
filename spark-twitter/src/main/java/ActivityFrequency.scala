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
import twitter4j.json.DataObjectFactory
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale

/*
 * Count the number of tweets, retweets, mentions, hashtags, and urls
 */
object ActivityFrequency {
  
  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"

  // Flag for the scale we are counting in
  object TimeScale extends Enumeration {
    type TimeScale = Value
    val MINUTE, HOURLY, DAILY = Value
  }

  /**
   * Print the usage message
   */
  def printHelp() : Unit = {
    println("Usage: spark-submit " + this.getClass.getCanonicalName + "<-m|-h|-d> <input_file> <output_dir> [numPartitions]")
    println("\t -m \t count tweets per minute")
    println("\t -h \t count tweets per hour")
    println("\t -d \t count tweets per day")
  }

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Activity Frequency")
    val sc = new SparkContext(conf)

    val timeScaleStr = args(0)
    val dataPath = args(1)
    val outputPath = args(2)

    // Validate and set time scale
    var timeScale = TimeScale.MINUTE
    if ( timeScaleStr == "-m" ) {
      timeScale = TimeScale.MINUTE
    } else if ( timeScaleStr == "-h" ) {
      timeScale = TimeScale.HOURLY
    } else if ( timeScaleStr == "-d" ) {
      timeScale = TimeScale.DAILY
    } else {
      printHelp()
      sys.exit(1)
    }
    
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
    
    // Only keep non-null status with text
    val tweetsFiltered = tweets.filter(status => {
        status != null &&
        status.getText != null &&
        status.getText.size > 0
      })

    // For each status, create a tuple with its creation time (flattened to the
    //  correct time scale) and a 1 for counting
    val timedTweets = tweetsFiltered.map(status => {
      val rtCount = if (status.isRetweet) 1 else 0
      val mentionCount = status.getUserMentionEntities.size
      val urlCount = status.getURLEntities.size
      val mediaCount = status.getMediaEntities.size
      val hashtagCount = status.getHashtagEntities.size
      (convertTimeToSlice(status.getCreatedAt, timeScale), (1, rtCount, mentionCount, urlCount, mediaCount, hashtagCount))
    })

    // Sum up the different counts
    val datedCounts = timedTweets.reduceByKey((l, r) => {
      (l._1 + r._1, l._2 + r._2, l._3 + r._3, l._4 + r._4, l._5 + r._5, l._6 + r._6)
    })
    
    // Convert to a CSV string and save
    datedCounts.map(tuple => {
      val date = dateFormatter(tuple._1)

      val counts = tuple._2

      val tweetCount = counts._1
      val rtCount = counts._2
      val mentionCount = counts._3
      val urlCount = counts._4
      val mediaCount = counts._5
      val hashtagCount = counts._6

        f"$date%s, $tweetCount%d, $rtCount%d, $mentionCount%d, $urlCount%d, $mediaCount%d, $hashtagCount%d"
      }).saveAsTextFile(outputPath)
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
