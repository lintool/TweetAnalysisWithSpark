package edu.umd.cs.hcil.spark.analytics

/**
 * Created by cbuntain on 7/10/15.
 */

import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils
import org.apache.spark.{SparkContext, _}
import twitter4j.Status
import org.apache.lucene.analysis.core.{SimpleAnalyzer, StopAnalyzer}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.simple.SimpleQueryParser
import org.apache.spark.rdd.RDD

object TweetQuery {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Lucene Tweet Query")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val keywordPath = args(2)

    val queries = scala.io.Source.fromFile(keywordPath).getLines.toList

    println("Queries:")
    queries.foreach(s => println("\t" + s))
    println()

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }

    val tweets = twitterMsgs.map(line => {
      (line, JsonUtils.jsonToStatus(line))
    })

    val tweetTexts = tweets.filter(tuple => {
      val status = tuple._2

      status != null &&
      status.getText.length > 0
    })

    val relevantTweetJson = querier(queries, tweetTexts, 0d).map(pair => pair._1)

    relevantTweetJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def querier(queries : List[String], statusList : RDD[(String,Status)], threshold : Double) : RDD[(String,Status)] = {
    // Pseudo-Relevance feedback
    val scoredPairs = statusList.mapPartitions(iter => {
      // Construct an analyzer for our tweet text
      val analyzer = new StandardAnalyzer()
      val parser = new StandardQueryParser()

      // Use OR to be consistent with Gnip
      parser.setDefaultOperator(org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator.AND)

      iter.map(pair => {
        val status = pair._2
        val text = status.getText

        // Construct an in-memory index for the tweet data
        val idx = new MemoryIndex()

        idx.addField("content", text.toLowerCase(), analyzer)

        var score = 0.0d
        for ( q <- queries ) {
          score = score + idx.search(parser.parse(q, "content"))
        }

        (pair, score)
      })
    }).filter(tuple => tuple._2 > threshold)
      .map(tuple => tuple._1)

    return scoredPairs
  }
}
