package edu.umd.cs.hcil.spark.analytics.sentiment

/**
  * Created by cbuntain on 12/1/16.
  */
trait SentimentClassifier {

  /**
    * Given a tweet, determine its sentiment
    *
    * @param status Status to test
    */
  def calculateSentiment(status : String) : Double

}
