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

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.util.CoreMap
import java.util.Properties

object Sentiment {

  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"
  
  // Flag for the scale we are counting in
  object TimeScale extends Enumeration {
    type TimeScale = Value
    val MINUTE, HOURLY, DAILY = Value
  }
  
  // This class wraps the instantiation of Stanford's CoreNLP pipeline, which
  //  we use for lazy initialization on each node's JVM
  class CoreNlpWrapper {
    val pipelineProps = new Properties()
    pipelineProps.setProperty("ssplit.eolonly", "true")
    pipelineProps.setProperty("annotators", "parse, sentiment")
    pipelineProps.setProperty("enforceRequirements", "false")

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
  
  /**
   * Print the usage message
   */
  def printHelp() : Unit = {
      println("Usage: spark-submit " + this.getClass.getCanonicalName + "<-m|-h|-d> <input_file> <output_dir> [numPartitions]")
      println("\t -m \t sentiment for tweets per minute")
      println("\t -h \t sentiment for tweets per hour")
      println("\t -d \t sentiment for tweets per day")    
  }

  /**
   * Calculate the average sentiment per minute/hour/day from a set of tweets
   *
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
 
    if ( args.size < 3 ) {
      printHelp()
      sys.exit(1)
    }
    
    val conf = new SparkConf().setAppName("Sentiment")
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
    //  correct time scale) and its text
    val timedTweetText : RDD[Tuple2[Date, String]] = tweetsFiltered.map(status => {
        val date = convertTimeToSlice(status.getCreatedAt, timeScale)
        (date, status.getText)
        
      })

    // Map (date, tweet) to (date, avg sentiment) for that tweet
    val timedSentiments : RDD[Tuple2[Date, Tuple3[Double, Int, Int]]] = 
      timedTweetText.mapValues(tweetText => {
        
        val tokenizer = TransientCoreNLP.coreNLP.tokenizer
        val pipeline = TransientCoreNLP.coreNLP.pipeline
        
        var posCount = 0
        var negCount = 0
        var scoreSum = 0d
        var sentenceCount = 0
        
        // create an empty Annotation just with the given text
        val document : Annotation = tokenizer.process(tweetText)

        // run all Annotators on this text
        pipeline.annotate(document)

        // For each sentence, calculate the sentiment and add to sum
        val sentences = document.get(classOf[SentencesAnnotation]).asScala
        for (sentence <- sentences) {

          val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
          val score = RNNCoreAnnotations.getPredictedClass(tree)

          scoreSum += score
          sentenceCount += 1
          
          if ( score > 2 ) {
            posCount += 1
          }
          else if ( score < 2 ) {
            negCount += 1
          }
        }
        
        // The sentiment scale for the Stanford CoreNLP library is as follows:
        // { 0:"Very Negative", 1:"Negative", 2:"Neutral", 3:"Positive", 4:"Very Positive"};
        
        // Calculate average sentiment score using summed sentiment over sentences.
        //  If no sentences were found, default to 2, which is neutral
        val avgScore = if ( sentenceCount> 0 ) { scoreSum / sentenceCount } else { 2 }
        
        // Return tuple of average sentiment, and count of positive and negative
        //  sentences
        (avgScore, posCount, negCount)
      })
    
    // Find the per-window average sentiment, and sum +/- counts
    val timedSentimentMeans : RDD[Tuple2[Date, Tuple3[Double, Int, Int]]] = 
      timedSentiments.groupByKey.mapValues(sentValues => {
        val sums = sentValues.reduce((l, r) => (l._1 + r._1, l._2 + r._2, l._3 + r._3))
        val count = sentValues.size
        
        val sentMean = sums._1.toDouble / count
        (sentMean, sums._2, sums._3)
      })
    
    // Pull out just the times
    val times = timedSentimentMeans.map(tuple => {
        tuple._1
      })
    
    // Find the min and max times, so we can construct a full list
    val minTime = times.reduce((l, r) => {
        if ( l.compareTo(r) < 1 ) {
          l
        } else {
          r
        }
      })
    println("Min Time: " + minTime)
    
    val maxTime = times.reduce((l, r) => {
        if ( l.compareTo(r) > 0 ) {
          l
        } else {
          r
        }
      })
    println("Max Time: " + maxTime)
    
    // Create keys for EACH time between min and max, then parallelize it for
    //  merging with actual data
    //  NOTE: This step likely isn't necessary if we're dealing with the full
    //  1% stream, but filtered streams aren't guarateed to have data in each
    //  time step.
    val fullKeyList = constructDateList(minTime, maxTime, timeScale)
    val fullKeyRdd : RDD[Tuple2[Date, Tuple3[Double, Int, Int]]] = 
      sc.parallelize(fullKeyList).map(key => (key, (0,0,0)))
    
    // Merge the full date list and regular data
    val withFullDates = timedSentimentMeans.union(fullKeyRdd)
    val mergedDates : RDD[Tuple2[Date, Tuple3[Double, Int, Int]]] = 
      withFullDates.reduceByKey((l, r) => (l._1 + r._1, l._2 + r._2, l._3 + r._3))
    
    // Sort for printing
    val sliceCounts = mergedDates.sortByKey()
    
    // Convert to a CSV string and save
    sliceCounts.map(tuple => {
        val (mean, posCount, negCount) = tuple._2
        dateFormatter(tuple._1) + ", " + mean + ", " + posCount + ", " + negCount
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
   * @param date The date to flatten
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
