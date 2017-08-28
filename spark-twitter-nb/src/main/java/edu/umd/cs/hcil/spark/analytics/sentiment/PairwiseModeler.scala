package edu.umd.cs.hcil.spark.analytics.sentiment

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.stat.Statistics

/**
  * Created by cbuntain on 8/11/17.
  */
class PairwiseModeler {



}

object PairwiseModeler {

  def tokenFilter(docDF : DataFrame, featureColumn : String, stopwords : Array[String]) : DataFrame = {

    // Split each document into words
    val tokens = new RegexTokenizer().
      setGaps(false). // tells the tokenizer to look for tokens matching the pattern rather than finding whitespace
      setPattern("#?\\p{L}{4,}+").
      setInputCol(featureColumn).
      setOutputCol("words").
      transform(docDF)

    // Filter out stopwords
    val filteredTokens = new StopWordsRemover().
      setStopWords(stopwords).
      setCaseSensitive(false).
      setInputCol("words").
      setOutputCol("filtered").
      transform(tokens)

    return filteredTokens
  }

  def buildModel(filteredTokens : DataFrame) : CountVectorizerModel = {

    // Limit to top `vocabSize` most common words and convert to word count vector features
    val cvModel = new CountVectorizer().
      setInputCol("filtered").
      setOutputCol("features").
      setMinTF(0).
      setMinDF(0).
      fit(filteredTokens)

    return cvModel
  }

  def main(args : Array[String]) : Unit = {

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    val csv_file = args(0)
    val outputPath = args(1)
    val stops = args(2)

    println("Input: " + csv_file)
    println("Output: " + outputPath)

    val stopwords = scala.io.Source.fromFile(stops).getLines.toArray

    //initialize CSVParser object
    val csv_format = org.apache.commons.csv.CSVFormat.DEFAULT.withSkipHeaderRecord()
    val parser = org.apache.commons.csv.CSVParser.
      parse(new File(csv_file), java.nio.charset.StandardCharsets.UTF_8, csv_format)

    val records = ListBuffer.empty[(Long,Int,String)]

    for ( line <- parser ) {
      if ( line.getRecordNumber > 1 ) {
        val tweet_id = line.get(1).toLong
        val tweet_degree = line.get(2).toInt
        val tweet_text = line.get(3)

        records += Tuple3(tweet_id, tweet_degree, tweet_text)
      }
    }

    val min_degree = records.map(t => t._2).reduce((l, r) => Math.min(l, r)).toDouble
    val max_degree = records.map(t => t._2).reduce((l, r) => Math.max(l, r)).toDouble
    println("Min Degree: " + min_degree)
    println("Max Degree: " + max_degree)

    val normed = records.
      map(t => (t._1, t._2, t._3, (t._2.toDouble - min_degree)/(max_degree - min_degree)))

    println("Record Count: " + normed.length)

    val labeled_data = sc.parallelize(normed)

    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    val docDF = labeled_data.toDF("docId", "degree", "text", "norm")

    val filteredTokens = tokenFilter(docDF, "text", stopwords)
    val cvModel = buildModel(filteredTokens)

    val countVectors = cvModel.transform(filteredTokens).
      select("docId", "features", "norm").
      cache()

    // Building the model
    val numIterations = 1000
    val algorithm = new LinearRegression()
      .setMaxIter(numIterations)
      .setFeaturesCol("features")
      .setLabelCol("norm")
      .setPredictionCol("prediction")

    val model = algorithm.fit(countVectors)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    // Save and load model
    model.save(outputPath + "_lr.model")
    cvModel.save(outputPath + "_cv.model")

    val predicted = model.transform(countVectors)
    val preds = predicted.select("prediction").rdd.map(r => r.getDouble(0))
    val acts = labeled_data.map(t => t._4)

    val correlation: Double = Statistics.corr(
      preds,
      acts,
      "pearson")
    println(s"Correlation is: $correlation")
  }
}