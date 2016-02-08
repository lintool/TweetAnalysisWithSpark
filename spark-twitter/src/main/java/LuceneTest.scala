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

import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.simple.SimpleQueryParser

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

object LuceneTest {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("TweetFrequency")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val query = args(1)
    
    val twitterMsgs = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgs.partitions.size)
    
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

    val tweetTextPairs = tweets.map(status => (status.getId, status.getText))

    val scoredPairs = tweetTextPairs.mapPartitions(iter => {
      // Construct an analyzer for our tweet text
      val analyzer = new SimpleAnalyzer()
      val parser = new SimpleQueryParser(analyzer, "content")

      iter.map(pair => {
        val id = pair._1
        val text = pair._2

        // Construct an in-memory index for the tweet data
        val idx = new MemoryIndex()

        idx.addField("content", text.toLowerCase(), analyzer)

        val score = idx.search(parser.parse(query))

        (id, text, score)
      })
    }).filter(tuple => tuple._3 > 0.0f)

    val collectedMatches = scoredPairs.collect().sortBy(tuple => tuple._3).reverse

    for ( tup <- collectedMatches ) {
      println(tup._1 + "\t" + tup._2.replace("\n", " ") + "\t" + tup._3)
    }

    println("Top 10:")
    for ( tup <- collectedMatches.take(10) ) {
      println(tup._1 + "\t" + tup._2.replace("\n", " ") + "\t" + tup._3)
    }

    val scores = collectedMatches.map(t => t._3)

    println("Match count: " + collectedMatches.size)
    println("Score ranges:")
    println(scores.max)
    println(scores.min)
  }

}
