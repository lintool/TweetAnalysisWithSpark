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
import java.text.SimpleDateFormat;
import java.util.Locale;

object UsersByDay {
  
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
    
    val conf = new SparkConf().setAppName("UserFrequency")
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
          DataObjectFactory.createStatus(line)
        } catch {
          case e : Exception => null
        }
      })
    
    // Only keep non-null status with text and have at least 1 friend or follower
    val tweetsFiltered = tweets.filter(status => {
      status != null &&
      status.getText != null &&
      status.getUser.getFollowersCount + status.getUser.getFriendsCount > 0 &&
      status.getUser.getStatusesCount > 0
    })

    // Convert to tuple of (Date, User)
    val datedUsers = tweetsFiltered.map(status => {
      (convertTimeToSlice(status.getCreatedAt, TimeScale.DAILY), status.getUser)
    }).mapValues(u => {
      val friendFollowSum : Double = u.getFriendsCount + u.getFollowersCount
      val frRatio = if ( friendFollowSum > 0 ) {
        u.getFriendsCount / friendFollowSum
      } else {
        0d
      }

      val isSeeker = if ( u.getFriendsCount > u.getFollowersCount ) { 1 } else { 0 }

      (1,
        frRatio,
        BigInt(u.getFriendsCount),
        BigInt(u.getFollowersCount),
        BigInt(u.getListedCount),
        BigInt(u.getStatusesCount),
        isSeeker)
    })

    val datedSums = datedUsers.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4, t1._5 + t2._5, t1._6 + t2._6, t1._7 + t2._7)
    })

    val userStats : RDD[(Date, (Int, Int, Double, Double, Double))] = datedSums.mapValues(l => {
      val count : Double = l._1

      if ( count > 0 ) {

        val avgRatio: Double = l._2 / count
        val avgList: Double = l._5.doubleValue / count
        val avgTweet: Double = l._6.doubleValue / count

        (l._1, l._7, avgRatio, avgList, avgTweet)
      } else {
        (0, 0, 0d, 0d, 0d)
      }
    }).sortByKey()
    
    // Convert to a CSV string and save
    userStats.map(tuple => {
      val date = dateFormatter(tuple._1)

      val count = tuple._2._1
      val seekerCount = tuple._2._2
      val avgRatio = tuple._2._3
      val avgList = tuple._2._4
      val avgTweet = tuple._2._5

      f"$date%s,$count%d,$seekerCount%d,$avgTweet%f,$avgRatio%f,$avgList%f"
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
