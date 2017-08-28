package edu.umd.cs.hcil.spark.analytics

import org.apache.spark.ml.attribute.{NominalAttribute, NumericAttribute}
import org.apache.spark.ml.classification.{GBTClassifier, OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, IDF, Tokenizer}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by cbuntain on 9/1/16.
  */
object MultiClassifier {

  case class Sample(label : Double, tweet : String, otherData : Array[String], id : String)

  /**
    * Convert an RDD of Sample objects into a DataFrame suitable for machine learning.
    *
    * @param samplesRdd An RDD of Sample objects that can be converted into a DataFrame
    * @param labels The possible label set
    * @param sqlContext The SQL context to use for converting to DataFrames
    */
  def convertRdd2Df(samplesRdd : RDD[Sample], labels : Array[String], sqlContext : SQLContext) : DataFrame = {
    val meta = NominalAttribute.
      defaultAttr.
      withName("label").
      withValues(labels).
      toMetadata

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val samples = samplesRdd.toDF()
    val samplesWithMeta = samples.withColumn("label", $"label".as("label", meta))

    return samplesWithMeta
  }

  /**
    * Convert an RDD of Sample objects into a DataFrame suitable for machine learning.
    * Then train a classifier using this data and return the resulting model.
    *
    * @param samplesRdd An RDD of Sample objects that can be converted into a DataFrame
    * @param labels The possible label set
    * @param sqlContext The SQL context to use for converting to DataFrames
    * @param iterations Number of training iterations
    */
  def trainModel(samplesRdd : RDD[Sample], labels : Array[String], sqlContext : SQLContext, iterations : Int = 10) : Transformer = {
    val samplesWithMeta = convertRdd2Df(samplesRdd, labels, sqlContext)

    return trainModel(samplesWithMeta, sqlContext, iterations)
  }

  /**
    * Convert an RDD of Sample objects into a DataFrame suitable for machine learning.
    * Then train a classifier using this data and return the resulting model.
    *
    * @param samplesWithMeta DataFrame of Sample objects for training
    * @param sqlContext The SQL context to use for converting to DataFrames
    * @param iterations Number of training iterations
    */
  def trainModel(samplesWithMeta : DataFrame, sqlContext : SQLContext, iterations : Int) : Transformer = {

    // Lowercase and tokenize tweet text
    val featureTokenizer = new Tokenizer().
      setInputCol("tweet").
      setOutputCol("tokenizedTweet")

    // For converting tokens into counts
    val featureCounter = new CountVectorizer().
      setInputCol("tokenizedTweet").
      setOutputCol("countVector")

    val featureIDF = new IDF().
      setInputCol("countVector").
      setOutputCol("idfVector")

    val featureColName = "idfVector"

    // Train a GBT model.
    val clf = new RandomForestClassifier().
      setLabelCol("label").
      setFeaturesCol(featureColName).
      setNumTrees(iterations)

    // Chain transformers and classifier in a Pipeline
    val pipeline = new Pipeline().
      setStages(Array(featureTokenizer, featureCounter, featureIDF, clf))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(samplesWithMeta)

    return model
  }

  /**
    * Convert an RDD of Sample objects into a DataFrame suitable for machine learning.
    * Then train a classifier using this data and return the resulting model.
    *
    * @param samplesRdd DataFrame of Sample objects for training
    * @param labels The possible label set
    * @param sqlContext The SQL context to use for converting to DataFrames
    * @param runCount Number of folds to run in random subset selection
    * @param learnerIterations Number of training iterations
    */
  def assessClassifier(samplesRdd : RDD[Sample], labels : Array[String], sqlContext : SQLContext, runCount : Int, learnerIterations : Int = 10) : (Double, Double) = {

    // Convert samples RDD to DataFrame
    val samples = convertRdd2Df(samplesRdd, labels, sqlContext)

    var scores : List[(Double, Double)] = List()

    for ( i <- 0 until runCount ) {
      // Split the data into training and test sets (30% held out for testing)
      val Array(trainingData, testData) = samples.randomSplit(Array(0.7, 0.3))

      val model = trainModel(trainingData, sqlContext, learnerIterations)

      // Make predictions.
      val predictions = model.transform(testData)

      // Select (prediction, true label) and compute test error
      val precEval = new MulticlassClassificationEvaluator().
        setLabelCol("label").
        setPredictionCol("prediction").
        setMetricName("precision")

      // Select (prediction, true label) and compute test error
      val recallEval = new MulticlassClassificationEvaluator().
        setLabelCol("label").
        setPredictionCol("prediction").
        setMetricName("recall")

      val precision = precEval.evaluate(predictions)
      val recall = recallEval.evaluate(predictions)

      scores = scores :+ (precision, recall)
    }

    val summedScores = scores.reduce((l, r) => {
      val precSum = l._1 + r._1
      val recSum = l._2 + r._2

      (precSum, recSum)
    })

    val avgPrec = summedScores._1 / scores.size
    val avgRec = summedScores._2 / scores.size

    return (avgPrec, avgRec)
  }

}
