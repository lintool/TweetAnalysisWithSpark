/**
 * Created by cbuntain on 7/10/15.
 */

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}
import java.io.FileWriter
import org.apache.commons.csv.{CSVPrinter, CSVFormat}
import org.apache.spark.{SparkContext, _}
import org.apache.spark.mllib.feature.{Word2Vec, _}
import twitter4j.json.DataObjectFactory
import scala.collection.JavaConverters._

object DailySynonyms {

  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2Vec Model Builder")
    val sc = new SparkContext(conf)

    if ( args.size < 7 ) {
      println("Usage: DailySynonyms <data_path> <target_keyword> <synonym_count> <output_file> <minCount> <w2vPart> <vectorSize> [partitions]")

      sys.exit(1)
    }

    val dataPath = args(0)
    val targetKeyword = args(1)
    val synonymCount = args(2).toInt
    val outputPath = args(3)
    val minCount = args(4).toInt
    val w2vPart = args(5).toInt
    val vectorSize = args(6).toInt

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 5 ) {
      val initialPartitions = args(5).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

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

    val timedTexts = tweets.filter(status => {
      status.isRetweet == false && status.getText.length > 0
    }).map(status => {
      (convertTimeToSlice(status.getCreatedAt), tokenize(status.getText).toList)
    })
    timedTexts.persist()

    // Find the min and max dates from the data
    val times = timedTexts.map(tuple => tuple._1)
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

    val timeList = constructDateList(minTime, maxTime)

    // List of synonyms for target keyword
    var dateSynonymMap : Map[Date, Iterable[String]] = Map.empty
    for ( time <- timeList ) {

      val dailyTweets = timedTexts.filter(tuple => tuple._1.compareTo(time) == 0).map(tuple => tuple._2)

      val w2v = new Word2Vec
      w2v.setMinCount(minCount)
      w2v.setNumPartitions(w2vPart)
      w2v.setVectorSize(vectorSize)
      val model: Word2VecModel = w2v.fit(dailyTweets)

      var syns : Iterable[String] = List.empty

      try {
        syns = model.findSynonyms(targetKeyword, synonymCount).map(tuple => tuple._1)
      } catch {
        case e : Exception => null
      }

      dateSynonymMap = dateSynonymMap + (time -> syns)
    }

    val outputFileWriter : FileWriter = new FileWriter(outputPath)
    val writer = new CSVPrinter(outputFileWriter, CSVFormat.DEFAULT)
    for ( time <- timeList ) {
      val dateString : String = dateFormatter(time)
      val elements : List[String] = List(dateString) ++ dateSynonymMap(time).toList

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

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }

  /**
   * Flatten timestamp to the appropriate scale
   *
   * @param time The date to flatten
   */
  def convertTimeToSlice(time : Date) : Date = {
    val cal = Calendar.getInstance
    cal.setTime(time)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 0)

    return cal.getTime
  }

  /**
   * Build the full date list between start and end
   *
   * @param startDate The first date in the list
   * @param endDate The last date in the list
   */
  def constructDateList(startDate : Date, endDate : Date) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)

    var l = List[Date]()

    while(cal.getTime.before(endDate)) {
      l = l :+ cal.getTime
      cal.add(Calendar.DATE, 1)
    }
    l = l :+ endDate

    return l
  }

}
