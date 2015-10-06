/**
 * Created by cbuntain on 7/10/15.
 */

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.lang.reflect.Type
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

object KeywordFinder {

  def gson = { new Gson() }
  val mapType : Type = new TypeToken[java.util.HashMap[String,Object]]() {}.getType()

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KeywordFinder")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val keywordPath = args(2)

    val keywords = scala.io.Source.fromFile(keywordPath).getLines.toList

    println("Searching for:")
    keywords.foreach(s => println("\t" + s))
    println()

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweets = twitterMsgs.map(line => (line, jsonToMap(line))).filter(item => item._2 != null)
    val tweetTexts = tweets.filter(tuple => {
      val jsonMap = tuple._2

      jsonMap.contains("text") &&
        jsonMap("text").asInstanceOf[String].length > 0
    })

    val englishTweets = tweetTexts.filter(tuple => {
      val jsonMap = tuple._2

      jsonMap.contains("lang") &&
        jsonMap("lang") == "en"
    })

    val relevantTweetJson = englishTweets.filter(tuple => {
      keywordFilter(keywords, tuple._2("text").asInstanceOf[String])
    }).map(tuple => tuple._1)

    relevantTweetJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def keywordFilter(keywords : List[String], searchString : String) : Boolean = {
    val tokens = tokenize(searchString)
    val intersection = tokens.intersect(keywords)

    return intersection.length > 0
  }

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }

  def jsonToMap(line : String) : Map[String, Object] = {
    try {
      val m : java.util.HashMap[String,Object] = gson.fromJson(line, mapType)
      return m.asScala.toMap
    } catch {
      case npe : NullPointerException => return null
      case e : Exception =>
        println("Unable to parse JSON:" + e)
        return null
    }
  }

}
