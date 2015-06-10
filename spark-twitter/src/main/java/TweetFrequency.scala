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

object TweetFrequency {
  
  object TimeScale extends Enumeration {
    type TimeScale = Value
    val MINUTE, HOURLY, DAILY = Value
  }
  
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
 
    if ( args.size < 3 ) {
      printHelp()
      sys.exit(1)
    }
    
    val conf = new SparkConf().setAppName("TweetFrequency")
    val sc = new SparkContext(conf)
    
    val timeScaleStr = args(0)
    val dataPath = args(1)
    val outputPath = args(2)
    
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
    
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size
    
    val tweets = twitterMsgs.map(line => {
        try {
          DataObjectFactory.createStatus(line)
        } catch {
          case e : Exception => null
        }
      })
    val tweetsFiltered = tweets.filter(status => {
        status != null &&
        status.getText != null &&
        status.getText.size > 0
      })
    val timedTweets = tweetsFiltered.map(status => (convertTimeToSlice(status.getCreatedAt, timeScale), 1))
    
    val groupedCounts : RDD[Tuple2[Date, Int]] = timedTweets.reduceByKey((l, r) =>
      l + r
    )
    
    val times = groupedCounts.map(tuple => {
        tuple._1
      })
    val minTime = times.reduce((l, r) => {
        if ( l.compareTo(r) < 1 ) {
          l
        } else {
          r
        }
      })
    printf("Min Time: " + minTime)
    
    val maxTime = times.reduce((l, r) => {
        if ( l.compareTo(r) > 0 ) {
          l
        } else {
          r
        }
      })
    printf("Max Time: " + maxTime)
    
    val fullKeyList = constructDateList(minTime, maxTime)
    val fullKeyRdd : RDD[Tuple2[Date, Int]] = 
      sc.parallelize(fullKeyList).map(key => (key, 0))
    
    val withFullDates = groupedCounts.union(fullKeyRdd)
    val mergedDates = withFullDates.reduceByKey((l, r) => l + r)
    
    // Tells us the number messages per slice
    val sliceCounts = mergedDates.sortByKey()
    
    sliceCounts.saveAsTextFile(outputPath)
  }
  
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
  
  def constructDateList(startDate : Date, endDate : Date) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)
    
    var l = List[Date]()
    
    while(cal.getTime.before(endDate)) {
      l = l :+ cal.getTime
      cal.add(Calendar.DATE, 1)
//      cal.add(Calendar.HOUR, 1)
    }
    l = l :+ endDate
    
    return l
  }

}
