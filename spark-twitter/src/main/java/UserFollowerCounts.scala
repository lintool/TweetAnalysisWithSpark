/**
 * Created by cbuntain on 7/10/15.
 */

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.{Locale, Calendar, Date}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import twitter4j.json.DataObjectFactory

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.lang.reflect.Type
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

object UserFollowerCounts {

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
    println("Usage: spark-submit " + this.getClass.getCanonicalName + "<-m|-h|-d> <input_file> <output_dir> <user_id_file> [numPartitions]")
    println("\t -m \t count tweets per minute")
    println("\t -h \t count tweets per hour")
    println("\t -d \t count tweets per day")
  }


  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Track User Follower Counts")
    val sc = new SparkContext(conf)

    val timeScaleStr = args(0)
    val dataPath = args(1)
    val outputPath = args(2)
    val userIdPath = args(3)

    val userIds : Set[Long] = scala.io.Source.fromFile(userIdPath).getLines.map(s => s.toLong).toSet

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

    println("Searching for tweets from:")
    userIds.foreach(s => println("\t" + s))
    println()

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 5 ) {
      val initialPartitions = args(5).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweets : RDD[(Date,(Int, Int))] = twitterMsgs.flatMap(line => {
      var pairs = List.empty[(Date,(Int, Int))]

      try {
        val status = DataObjectFactory.createStatus(line)

        if ( userIds.contains(status.getUser.getId) ) {
          val tuple = (convertTimeToSlice(status.getCreatedAt, timeScale), (status.getUser.getFollowersCount, 1))

          pairs = List(tuple)
        } else if ( status.isRetweet && userIds.contains(status.getRetweetedStatus.getUser.getId) ) {
          val tuple = (convertTimeToSlice(status.getCreatedAt, timeScale), (status.getRetweetedStatus.getUser.getFollowersCount, 1))

          pairs = List(tuple)
        }
      } catch {
        case e : Exception =>
      }

      pairs
    })

    // We'll use this a lot
    tweets.cache()

    // Pull out just the times
    val times = tweets.map(tuple => tuple._1)
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

    val fullTimesRdd : RDD[(Date, (Int, Int))] =
      sc.parallelize(timeList).map(key => (key, (0, 0)))

    // Merge the full date list and regular data
    val withFullDates = tweets.union(fullTimesRdd)

    // For each time, find the tweets with the highest counts
    var dateTokenMap : Map[Date, (Int, Int, Double)] = Map.empty
    for ( time <- timeList ) {

      val thisTimesTweets : RDD[(Int, Int)] = withFullDates
        .filter(tuple => tuple._1.compareTo(time) == 0)
        .map(tuple => tuple._2)
      val retweetCounts = thisTimesTweets.reduce((l, r) => {
        val leftFollowerSum = l._1
        val leftTweetSum = l._2

        val rightFollowerSum = r._1
        val rightTweetSum = r._2

        (leftFollowerSum + rightFollowerSum, leftTweetSum + rightTweetSum)
      })

      val avgFollowerCount = if ( retweetCounts._2 > 0 ) { retweetCounts._1.toDouble / retweetCounts._2.toDouble } else { 0 }

      val data = (retweetCounts._1, retweetCounts._2, avgFollowerCount)
      dateTokenMap = dateTokenMap + (time -> data)
    }

    val outputFileWriter : FileWriter = new FileWriter(outputPath)
    val writer = new CSVPrinter(outputFileWriter, CSVFormat.DEFAULT)
    for ( time <- timeList ) {
      val dateString : String = dateFormatter(time)
      val tuple = dateTokenMap(time)
      val elements : List[String] = List(dateString, tuple._1.toString, tuple._2.toString, tuple._3.toString)

      writer.printRecord(elements.asJava)
    }
    writer.flush()
    outputFileWriter.close()
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
