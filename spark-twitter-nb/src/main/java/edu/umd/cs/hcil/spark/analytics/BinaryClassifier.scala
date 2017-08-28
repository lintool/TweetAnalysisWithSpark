package edu.umd.cs.hcil.spark.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.{Model, Pipeline, Transformer}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by cbuntain on 9/1/16.
  */
object BinaryClassifier {

  case class Sample(label : Double, tweet : String, otherData : Array[String], id : String)

  /**
    * Convert an RDD of Sample objects into a DataFrame suitable for machine learning.
    *
    * @param samplesRdd An RDD of Sample objects that can be converted into a DataFrame
    * @param sqlContext The SQL context to use for converting to DataFrames
    */
  def convertRdd2Df(samplesRdd : RDD[Sample], sqlContext : SQLContext) : DataFrame = {
    val meta = NominalAttribute.
      defaultAttr.
      withName("label").
      withValues("0.0", "1.0").
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
    * @param sqlContext The SQL context to use for converting to DataFrames
    * @param iterations Number of training iterations
    */
  def trainModel(samplesRdd : RDD[Sample], sqlContext : SQLContext, iterations : Int = 10) : Transformer = {
    val samplesWithMeta = convertRdd2Df(samplesRdd, sqlContext)

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
//    val clf = new GBTClassifier().
//      setLabelCol("label").
//      setFeaturesCol(featureColName).
//      setMaxIter(iterations)

    val clf = new RandomForestClassifier().
      setLabelCol("label").
      setFeaturesCol(featureColName).
      setNumTrees(iterations).
      setFeatureSubsetStrategy("auto").
      setImpurity("gini")

    // Chain transformers and classifier in a Pipeline
    val pipeline = new Pipeline().
      setStages(Array(featureTokenizer, featureCounter, featureIDF, clf))

    // Use ROC-AUC as the metric of interest
    val rocAucEval = new BinaryClassificationEvaluator().
      setLabelCol("label").
      setMetricName("areaUnderROC")

    // We use a ParamGridBuilder to construct a grid of parameters to search over
    val paramGrid = new ParamGridBuilder().
      addGrid(clf.maxBins, Array(25, 28, 31)).
      addGrid(clf.maxDepth, Array(4, 6, 8)).
      build()

    val cv = new CrossValidator().
      setEstimator(pipeline).
      setEvaluator(rocAucEval).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(10)

    // Train model.  This also runs the indexers.
    val model = cv.fit(samplesWithMeta)

    return model
  }

  /**
    * Convert an RDD of Sample objects into a DataFrame suitable for machine learning.
    * Then train a classifier using this data and return the resulting model.
    *
    * @param samplesRdd DataFrame of Sample objects for training
    * @param sqlContext The SQL context to use for converting to DataFrames
    * @param runCount Number of folds to run in random subset selection
    * @param learnerIterations Number of training iterations
    */
  def assessClassifier(samplesRdd : RDD[Sample], sqlContext : SQLContext, runCount : Int, learnerIterations : Int = 10) : (Double, Double, List[Transformer]) = {

    // Convert samples RDD to DataFrame
    val samples = convertRdd2Df(samplesRdd, sqlContext)

    var scores : List[(Double, Double)] = List()
    var models : List[Transformer] = List()

    for ( i <- 0 until runCount ) {
      // Split the data into training and test sets (30% held out for testing)
      val Array(trainingData, testData) = samples.randomSplit(Array(0.7, 0.3))

      val model = trainModel(trainingData, sqlContext, learnerIterations)
      models = models :+ model

      // Make predictions.
      val predictions = model.transform(testData)

      // Select (prediction, true label) and compute test error
      val precEval = new BinaryClassificationEvaluator().
        setLabelCol("label").
        setMetricName("areaUnderROC")

      // Select (prediction, true label) and compute test error
      val recallEval = new BinaryClassificationEvaluator().
        setLabelCol("label").
        setMetricName("areaUnderPR")

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

    return (avgPrec, avgRec, models)
  }

}
