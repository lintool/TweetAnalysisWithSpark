/**
 * Created by cbuntain on 7/10/15.
 */

import twitter4j.json.DataObjectFactory

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.lang.reflect.Type
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

object TweetExtractorViaRdd {

  def gson = { new Gson() }
  val mapType : Type = new TypeToken[java.util.HashMap[String,Object]]() {}.getType()

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Tweet Extractor")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val tweetIdPath = args(2)

    val tweetIds = sc.textFile(tweetIdPath).map(id => (id.toLong, null))

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

//    val tweets : RDD[(Long, String)] = twitterMsgs.flatMap(line => {
//      var pairs = List.empty[(Long, String)]
//
//      try {
//        val status = DataObjectFactory.createStatus(line)
//
//        if ( status.isRetweet == true ) {
//          pairs = List((status.getId, line), (status.getRetweetedStatus.getId, line))
//        } else {
//          pairs = List((status.getId, line))
//        }
//      } catch {
//        case e : Exception =>
//      }
//
//      pairs
//    })

    val tweets : RDD[(Long, String)] = twitterMsgs.map(line => {
      var pair : (Long, String) = null

      try {
        val status = DataObjectFactory.createStatus(line)
        pair = (status.getId, line)
      } catch {
        case e : Exception =>
      }

      pair
    }).filter(tuple => tuple != null)

    // Grab the first string for this tweet ID
    val tweetIdPairs = tweets.reduceByKey((l, r) => l)

    val relevantTweetJson : RDD[String] = tweetIdPairs.join(tweetIds).map(tuple => tuple._2._1)

    relevantTweetJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
