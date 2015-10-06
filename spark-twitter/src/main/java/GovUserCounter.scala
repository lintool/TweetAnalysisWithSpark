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
import twitter4j.Status
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale
import java.net.{URL,HttpURLConnection}

object GovUserCounter {
  
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

    HttpURLConnection.setFollowRedirects(false)

    val conf = new SparkConf().setAppName("GovUserCounter")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val targetUrlPattern = args(2)

    println("Searching for: [" + targetUrlPattern + "]")
    
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
          (line, DataObjectFactory.createStatus(line))
        } catch {
          case e : Exception => (line, null)
        }
      })
    
    // Only keep non-null status retweets from verified users
    val tweetsFiltered = tweets.filter(tuple => {
      val status = tuple._2

      status != null &&
        status.isRetweet &&
        status.getRetweetedStatus.getUser.isVerified
      })

    val pdRegex = "PD[^a-zA-Z]?".r

    val govRetweets = tweetsFiltered.filter(tuple => {
      val status = tuple._2

      val retweetedStatusUser = status.getRetweetedStatus.getUser

      val url = retweetedStatusUser.getURL
      val desc = retweetedStatusUser.getDescription

      var govFlag = false

      if ( url == null ) {
        govFlag = false
      } else if ( url.toLowerCase.contains("t.co") ) {
        try {
          val urlConn = new URL(url).openConnection()
          urlConn.setReadTimeout(5000)
          val finalUrl = urlConn.getHeaderField("Location").toLowerCase

          govFlag = (finalUrl != null && finalUrl.indexOf(targetUrlPattern) > -1)
          //          || desc.toLowerCase.indexOf("police") > -1
          //          || pdRegex.findFirstIn(desc) != None
        } catch {
          case e: Exception => govFlag = false
        }
      } else if ( url.toLowerCase.contains(targetUrlPattern) ) {
        govFlag = true
      }

      govFlag
    }).map(tuple => tuple._1)
    
    // Convert to a CSV string and save
    govRetweets.repartition(newPartitionSize)
      .saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
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
