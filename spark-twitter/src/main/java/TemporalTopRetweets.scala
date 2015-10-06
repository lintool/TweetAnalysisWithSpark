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

import java.io.FileWriter

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}

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

object TemporalTopRetweets {
  
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
      println("Usage: spark-submit " + this.getClass.getCanonicalName + "<-m|-h|-d> <input_file> <output_dir> <max_retweets> [numPartitions]")
      println("\t -m \t count tweets per minute")
      println("\t -h \t count tweets per hour")
      println("\t -d \t count tweets per day")    
  }

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
 
    if ( args.size < 4 ) {
      printHelp()
      sys.exit(1)
    }
    
    val conf = new SparkConf().setAppName("Top Retweets")
    val sc = new SparkContext(conf)
    
    val timeScaleStr = args(0)
    val dataPath = args(1)
    val outputPath = args(2)
    val maxTokens = args(3).toInt
    
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
    if ( args.size > 4 ) {
      val initialPartitions = args(4).toInt
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
    
    // Only keep non-null statuses that are retweets
    val tweetsFiltered = tweets.filter(status => {
        status != null &&
        status.isRetweet
      })
    
    // For each status, create a tuple with its creation time (flattened to the 
    //  correct time scale) and the tweet it contains
    val timedTweets = tweetsFiltered.map(status => {
      (convertTimeToSlice(status.getCreatedAt, timeScale), (status.getRetweetedStatus.getId, 1))
    })
    
    // Pull out just the times
    val times = timedTweets.map(tuple => tuple._1)
    val timeBounds = times.aggregate((new Date(Long.MaxValue), new Date(Long.MinValue)))((u, t) => {
      var min = u._1
      var max = u._2

      if ( t.before(min) ) {
        min = t
      }

      if ( t.after(max) ) {
        max = t
      }

      (min, max)
    },
      (u1, u2) => {
        var min = u1._1
        var max = u1._2

        if ( u2._1.before(min) ) {
          min = u2._1
        }

        if ( u2._2.after(max) ) {
          max = u2._2
        }

        (min, max)
      })
    val minTime = timeBounds._1
    val maxTime = timeBounds._2
    printf("Min Time: " + minTime + "\n")
    printf("Max Time: " + maxTime + "\n")

    val timeList = constructDateList(minTime, maxTime, timeScale)

    // Implicit ordering for sorting (tweet ID, count) tuples
    implicit val tupleValueOrdering = new Ordering[(Long, Int)] {
      override def compare(a: (Long, Int), b: (Long, Int)) = a._2.compare(b._2)
    }

    // Cache since we use it a bunch
    timedTweets.cache()

    // List of synonyms for target keyword
    var dateTokenMap : Map[Date, Iterable[(Long, Int)]] = Map.empty
    for ( time <- timeList ) {

      val thisTimesTweets : RDD[(Long, Int)] = timedTweets
        .filter(tuple => tuple._1.compareTo(time) == 0)
        .map(tuple => tuple._2)
      val retweetCounts = thisTimesTweets.reduceByKey((l, r) => l + r)

      val retweets : Iterable[(Long, Int)] = retweetCounts.top(maxTokens)

      dateTokenMap = dateTokenMap + (time -> retweets)
    }

    val outputFileWriter : FileWriter = new FileWriter(outputPath)
    val writer = new CSVPrinter(outputFileWriter, CSVFormat.DEFAULT)
    for ( time <- timeList ) {
      val dateString : String = dateFormatter(time)
      val elements : List[String] = List(dateString) ++ dateTokenMap(time).toList.map(tuple => tuple._1 + ":" + tuple._2)

      writer.printRecord(elements.asJava)
    }
    writer.flush()
    outputFileWriter.close()
  }

  /**
   * Convert a given date into a string using TIME_FORMAT
   *
   * @param filterTokens Tokens to remove
   * @param searchString The string to tokenize and sanitize
   */
  def tokenizeAndFilter(filterTokens : List[String], searchString : String) : List[String] = {
    val tokens = tokenize(searchString)
    val difference = tokens.diff(filterTokens)

    return difference.toList
  }

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
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
