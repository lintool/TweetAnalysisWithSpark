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
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature._

object Word2VecModel {

  def gson = { new Gson() }
  val mapType : Type = new TypeToken[java.util.HashMap[String,Object]]() {}.getType()

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2Vec Model Builder")
    val sc = new SparkContext(conf)

    if ( args.size < 5 ) {
      println("Usage: Word2VecModel <data_path> <model_output> <minCount> <w2vPart> <vectorSize> [partitions]")

      sys.exit(1)
    }

    val dataPath = args(0)
    val outputPath = args(1)
    val minCount = args(2).toInt
    val w2vPart = args(3).toInt
    val vectorSize = args(4).toInt

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 5 ) {
      val initialPartitions = args(5).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweets = twitterMsgs.map(line => jsonToMap(line)).filter(item => item != null)
    val tweetTexts = tweets.filter(status => {

      status.contains("retweeted_status") == false &&
        status.contains("text") &&
        status("text").asInstanceOf[String].length > 0
    })

    val tokenizedTweets : RDD[Iterable[String]] = tweetTexts.map(status => {
      tokenize(status("text").asInstanceOf[String]).toList
    })

    val w2v = new Word2Vec
    w2v.setMinCount(minCount)
    w2v.setNumPartitions(w2vPart)
    w2v.setVectorSize(vectorSize)
    val model : Word2VecModel = w2v.fit(tokenizedTweets)

    model.save(sc, outputPath)

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
