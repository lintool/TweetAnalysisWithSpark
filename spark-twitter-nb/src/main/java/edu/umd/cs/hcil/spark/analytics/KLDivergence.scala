package edu.umd.cs.hcil.spark.analytics

import edu.umd.cs.hcil.spark.analytics.utils.StatusTokenizer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.TwitterObjectFactory

/**
 * Created by cbuntain on 6/8/16.
 */
object KLDivergence {


  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KL Divergence")
    val sc = new SparkContext(conf)

    val foreDataPath = args(0)
    val backDataPath = args(1)
    val topK = args(2).toInt

    // Repartition if desired using the new partition count
    var foreMsgs = sc.textFile(foreDataPath)
    var backMsgs = sc.textFile(backDataPath)

    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      foreMsgs = foreMsgs.repartition(initialPartitions)
      backMsgs = backMsgs.repartition(initialPartitions)
    }

    val foreTokens = jsonTweetsToTokenFrequency(foreMsgs)
    val backTokens = jsonTweetsToTokenFrequency(backMsgs)

    val divergence = klDiverge(foreTokens, backTokens).sortBy(tup => tup._2, false).take(topK)

    println("Top " + topK + " divergent keywords:")
    for ( tokenTup <- divergence ) {
      println(tokenTup._1 + ", " + tokenTup._2)
    }
  }

  /**
    * Convert each JSON line in the file to a status using Twitter4j
    * Note that not all lines are Status lines, so we catch any exception
    * generated during this conversion and set to null since we don't care
    * about non-status lines.'
    *
    * @param rdd The JSON strings of tweets
    */
  def jsonTweetsToTokenFrequency(rdd : RDD[String]) : RDD[(String, Int)] = {

    // build the status from tweets
    val tweetTokens = rdd.map(line => {
      try {
        TwitterObjectFactory.createStatus(line)
      } catch {
        case e : Exception => null
      }
    }).filter(status => status != null)
      .flatMap(status => StatusTokenizer.tokenize(status).map(token => (token.toLowerCase, 1)))
      .reduceByKey((l, r) => l + r)

    return tweetTokens
  }

  /**
   * Convert each JSON line in the file to a status using Twitter4j
   * Note that not all lines are Status lines, so we catch any exception
   * generated during this conversion and set to null since we don't care
   * about non-status lines.'
   *
   * @param foreground RDD of foreground tokens and counts
   * @param background RDD of background tokens and counts
   */
  def klDiverge(foreground : RDD[(String, Int)], background : RDD[(String, Int)]) : RDD[(String, Double)] = {

    val foreTotal : Double = foreground.map(t => t._2).reduce((l, r) => l + r).toDouble
    val backTotal : Double = background.map(t => t._2).reduce((l, r) => l + r).toDouble

    println("Fore total: " + foreTotal)
    println("Back total: " + backTotal)

    val foreDist = foreground.map(tup => (tup._1, tup._2.toDouble / foreTotal))
    val backDist = background.map(tup => (tup._1, tup._2.toDouble / backTotal))

    val tokenDivergence = foreDist.join(backDist).map(tup => {
      val token = tup._1

      val probFore : Double = tup._2._1
      val probBack : Double = tup._2._2

      val klDiv : Double = probFore * (math.log(probFore) - math.log(probBack))

      (token, klDiv)
    })

    return tokenDivergence
  }
}
