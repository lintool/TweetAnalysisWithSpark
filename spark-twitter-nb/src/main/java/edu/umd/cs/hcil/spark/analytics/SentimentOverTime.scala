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

import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, Properties}

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.{SparkContext, _}
import org.apache.spark.rdd.RDD
import twitter4j.Status

import edu.umd.cs.hcil.spark.analytics.utils.{JsonUtils, TimeScale}

object SentimentOverTime {

  // This class wraps the instantiation of Stanford's CoreNLP pipeline, which
  //  we use for lazy initialization on each node's JVM
  class CoreNlpWrapper {
    val pipelineProps = new Properties()
    pipelineProps.setProperty("ssplit.eolonly", "true")
    pipelineProps.setProperty("annotators", "parse, sentiment")
    pipelineProps.setProperty("enforceRequirements", "false")
    pipelineProps.setProperty("sentiment.model", "model.ser.gz")

    val tokenizerProps = new Properties()
    tokenizerProps.setProperty("annotators", "tokenize, ssplit")

    val tokenizer : StanfordCoreNLP = new StanfordCoreNLP(tokenizerProps)
    val pipeline : StanfordCoreNLP = new StanfordCoreNLP(pipelineProps)
  }

  // A wicked hack to perform expensive instantiation of the CoreNLP pipeline.
  //  The transient annotation says to have a different coreNLP object in each
  //  JVM, and the lazy tag tells the JVM to instantiate the object on first use
  //  As a result, each node gets its own copy
  object TransientCoreNLP {
    @transient lazy val coreNLP = new CoreNlpWrapper()
  }

  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"
  
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
 
    if ( args.size < 3 ) {
      printHelp()
      sys.exit(1)
    }
    
    val conf = new SparkConf().setAppName("Sentiment Over Time")
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
    val tweets = twitterMsgs.map(JsonUtils.jsonToStatus(_))
    
    // Only keep non-null status with text
    val tweetsFiltered = tweets.filter(status => {
        status != null &&
        status.getText != null &&
        status.getText.size > 0
      })


    val timedText = tweetsFiltered.map(status => (status.getCreatedAt, status.getText))
    val results = calculateSentimentOverTime(timedText, timeScale)

    for ( d <- results.keys.toList.sorted ) {
      println(d, results(d))
    }
  }


  /**
    * Given an RDD of statuses, generate average sentiment values per date
    *
    * @param tweets Tweet list
    * @param scale Time scale
    */
  def calculateSentimentOverTime(tweets : RDD[(Date, String)], scale : TimeScale.Value) : Map[Date,Double] = {

    // For each status, create a tuple with its creation time (flattened to the
    //  correct time scale) and a 1 for counting
    val timedTweets = tweets.map(tup => (convertTimeToSlice(tup._1, scale), tup._2))
    val timeBounds = findDateBoundaries(timedTweets.map(tup => tup._1))
    val minTime = timeBounds._1
    val maxTime = timeBounds._2

    // Create keys for EACH time between min and max
    val fullKeyList = constructDateList(minTime, maxTime, scale)

    // Map (date, tweet) to (date, (tweet, avg sentiment, pos count, neg count, and neutral count)) for that tweet
    val timedTweetSentiments : RDD[(Date, (String, Double, Int, Int, Int))] =
      timedTweets.map(tup => {

        val date = tup._1
        val status = tup._2

        val sentTuple = calculateSentiment(status)

        // Return tuple of average sentiment, and count of positive and negative
        //  sentences
        (date, sentTuple)
      })

    // Sum and count the sentiment values per date
    val aggregatedDates = timedTweetSentiments.map(tup => (tup._1, tup._2._2))
      .aggregateByKey((0d, 0))(
        (accumSum, value) => {
          (accumSum._1 + value, accumSum._2 + 1)
        },
        (leftSum, rightSum) => {
          (leftSum._1 + rightSum._1, leftSum._2 + rightSum._2)
        }
      )

    val datedSentiment = aggregatedDates.map(tup => {
      val date = tup._1
      val avg = tup._2._1 / tup._2._2

      (date, avg)
    }).collectAsMap()

    val fullSentimentMap = fullKeyList.map(d => (d, datedSentiment.getOrElse(d, 0d))).toMap

    return fullSentimentMap
  }

  /**
    * Given a tweet, determine its sentiment and return its average sentiment
    * across all sentences, the number of positive sentences, the number of
    * negative sentences, and the number of neutral sentences.
    *
    * @param status Status to test
    */
  def calculateSentiment(status : String) : (String, Double, Int, Int, Int) = {

    val tokenizer = TransientCoreNLP.coreNLP.tokenizer
    val pipeline = TransientCoreNLP.coreNLP.pipeline

    var posCount = 0
    var negCount = 0
    var neutralCount = 0
    var scoreSum = 0d
    var sentenceCount = 0

    // create an empty Annotation just with the given text
    val document : Annotation = tokenizer.process(status)

    // run all Annotators on this text
    pipeline.annotate(document)

    // For each sentence, calculate the sentiment and add to sum
    val sentences = document.get(classOf[SentencesAnnotation]).asScala
    for (sentence <- sentences) {

      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])

      // The sentiment scale for the Stanford CoreNLP library is as follows:
      // { 0:"Very Negative", 1:"Negative", 2:"Neutral", 3:"Positive", 4:"Very Positive"};
      //  so we subtract 2 for scaling
      val score = RNNCoreAnnotations.getPredictedClass(tree) - 2.0d

      scoreSum += score
      sentenceCount += 1

      if ( score > 0 ) {
        posCount += 1
      }
      else if ( score < 0 ) {
        negCount += 1
      } else {
        neutralCount += 1
      }
    }

    // Calculate average sentiment score using summed sentiment over sentences.
    //  If no sentences were found, default to 2, which is neutral
    val avgScore = if ( sentenceCount > 0 ) { scoreSum / sentenceCount } else { 0.0d }

    return (status, avgScore, posCount, negCount, neutralCount)
  }

  /**
    * Given an RDD of (date, status) pairs, return the time boundaries
    *
    * @param tweetDates Tweet list
    */
  def findDateBoundaries(tweetDates : RDD[Date]) : (Date, Date) = {
    // Pull out just the times
    val times = tweetDates.map(d => {
      (d, d)
    })

    // Find the min and max times, so we can construct a full list
    val timeBounds = times.reduce((l, r) => {
      val newMin = if ( l._1.compareTo(r._1) < 1 ) {
        l._1
      } else {
        r._1
      }

      val newMax = if ( l._2.compareTo(r._2) < 1 ) {
        r._2
      } else {
        l._2
      }

      (newMin, newMax)
    })

    return timeBounds
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
