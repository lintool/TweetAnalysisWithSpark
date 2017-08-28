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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer, LDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import twitter4j.Status
import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils

/*
 * Count the number of tweets, retweets, mentions, hashtags, and urls
 */
object TwitterTopicModeler {

  /**
    * Print the usage message
    */
  def printHelp() : Unit = {
    println("Usage: spark-submit " + this.getClass.getCanonicalName + "<input_file> <output_dir> [numPartitions]")
  }

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Twitter Topic Modeler")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val stopwordPath = args(2)

    val stopwords = scala.io.Source.fromFile(stopwordPath).getLines.toList

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
      .filter(status => {
        status != null &&
        status.isRetweet == false &&
        status.getLang != null &&
        status.getLang.compareToIgnoreCase("en") == 0 &&
        getHashtagCount(status) <= 3 &&
        getUrlCount(status) <= 2
      })

    val numTopics = 100
    val ldaTuple = runLda(numTopics, tweets, stopwords.toArray, sc, 1, 1000)
    val ldaModel = ldaTuple._1
    val cvModel = ldaTuple._2

    ldaModel.save(sc, outputPath + "_lda.model")
    sc.parallelize(cvModel).saveAsObjectFile(outputPath + "_vocab.model")

    /**
      * Print results.
      */
    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (cvModel(term.toInt), weight) }
    }
    println(s"Topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }
  }

  /**
    * Count up activities
    *
    * @param numTopics Topic count
    * @param tweets RDD of tweets
    * @param stopwords List of stopwords to ignore
    * @param sc Spark context, needed for SQL context creation
    * @param minTF Minimum frequency a token must have in a document to be included in that document
    * @param minDF Minimum document frequency a token must have to be included
    */
  def runLda(numTopics : Int,
             tweets : RDD[Status],
             stopwords : Array[String],
             sc : SparkContext,
             minTF : Int,
             minDF : Int
            ) : (LDAModel, Array[String]) = {

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val docDF = tweets.map(status => (status.getText, status.getId)).toDF("text", "docId")

    // Split each document into words
    val tokens = new RegexTokenizer()
      .setGaps(false) // tells the tokenizer to look for tokens matching the pattern rather than finding whitespace
      .setPattern("#?\\p{L}{4,}+")
      .setInputCol("text")
      .setOutputCol("words")
      .transform(docDF)

    // Filter out stopwords
    val filteredTokens = new StopWordsRemover()
        .setStopWords(stopwords)
        .setCaseSensitive(false)
        .setInputCol("words")
        .setOutputCol("filtered")
        .transform(tokens)

    // Limit to top `vocabSize` most common words and convert to word count vector features
    val cvModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setMinTF(minTF)
      .setMinDF(minDF)
      .fit(filteredTokens)

    val countVectors = cvModel.transform(filteredTokens)
      .select("docId", "features")
      .rdd
      .map { case Row(docId: Long, features: MLVector) => (docId, Vectors.fromML(features)) }
      .cache()

    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer())
      .setK(numTopics)
      .setMaxIterations(75)
      .setDocConcentration(-1) // use default symmetric document-topic prior
      .setTopicConcentration(-1) // use default symmetric topic-word prior

    val startTime = System.nanoTime()
    val ldaModel = lda.run(countVectors)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    // Print training time
    println(s"Finished training LDA model.  Summary:")
    println(s"Training time (sec)\t$elapsed")
    println(s"==========")

    return (ldaModel, cvModel.vocabulary)
  }

  def getHashtagCount(status : Status) : Int = {
    val count = if ( status.getHashtagEntities != null ) {
      status.getHashtagEntities.size
    }  else {
      0
    }

    return count
  }

  def getUrlCount(status : Status) : Int = {
    val count = if ( status.getURLEntities != null ) {
      status.getURLEntities.size
    }  else {
      0
    }

    return count
  }

}
