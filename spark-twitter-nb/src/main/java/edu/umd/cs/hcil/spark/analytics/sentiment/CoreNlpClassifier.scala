package edu.umd.cs.hcil.spark.analytics.sentiment

import java.util.Properties
import scala.collection.JavaConverters._

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConverters._
/**
  * Created by cbuntain on 12/1/16.
  */
object CoreNlpClassifier extends SentimentClassifier {

  // This class wraps the instantiation of Stanford's CoreNLP pipeline, which
  //  we use for lazy initialization on each node's JVM
  class CoreNlpWrapper {
    val pipelineProps = new Properties()
    pipelineProps.setProperty("ssplit.eolonly", "true")
    pipelineProps.setProperty("annotators", "parse, sentiment")
    pipelineProps.setProperty("enforceRequirements", "false")
    pipelineProps.setProperty("sentiment.model", "model.ser.gz")

    val tokenizerProps = new Properties()
    tokenizerProps.setProperty("annotators", "tokenize, ssplit")

    val tokenizer : StanfordCoreNLP = new StanfordCoreNLP(tokenizerProps)
    val pipeline : StanfordCoreNLP = new StanfordCoreNLP(pipelineProps)
  }

  // A wicked hack to perform expensive instantiation of the CoreNLP pipeline.
  //  The transient annotation says to have a different coreNLP object in each
  //  JVM, and the lazy tag tells the JVM to instantiate the object on first use
  //  As a result, each node gets its own copy
  object TransientCoreNLP {
    @transient lazy val coreNLP = new CoreNlpWrapper()
  }

  /**
    * Given a tweet, determine its sentiment
    *
    * @param status Status to test
    */
  def calculateSentiment(status : String) : Double = {
    val coreTuple = calculateCoreNlpSentiment(status)

    return coreTuple._1
  }

  /**
    * Given a tweet, determine its sentiment and return its average sentiment
    * across all sentences, the number of positive sentences, the number of
    * negative sentences, and the number of neutral sentences.
    *
    * @param status Status to test
    */
  def calculateCoreNlpSentiment(status : String) : (Double, Int, Int, Int) = {

    val tokenizer = TransientCoreNLP.coreNLP.tokenizer
    val pipeline = TransientCoreNLP.coreNLP.pipeline

    var posCount = 0
    var negCount = 0
    var neutralCount = 0
    var scoreSum = 0d
    var sentenceCount = 0

    // create an empty Annotation just with the given text
    val document : Annotation = tokenizer.process(status)

    // run all Annotators on this text
    pipeline.annotate(document)

    // For each sentence, calculate the sentiment and add to sum
    val sentences = document.get(classOf[SentencesAnnotation]).asScala
    for (sentence <- sentences) {

      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])

      // The sentiment scale for the Stanford CoreNLP library is as follows:
      // { 0:"Very Negative", 1:"Negative", 2:"Neutral", 3:"Positive", 4:"Very Positive"};
      //  so we subtract 2 for scaling
      val score = RNNCoreAnnotations.getPredictedClass(tree) - 2.0d

      scoreSum += score
      sentenceCount += 1

      if ( score > 0 ) {
        posCount += 1
      }
      else if ( score < 0 ) {
        negCount += 1
      } else {
        neutralCount += 1
      }
    }

    // Calculate average sentiment score using summed sentiment over sentences.
    //  If no sentences were found, default to 2, which is neutral
    val avgScore = if ( sentenceCount > 0 ) { scoreSum / sentenceCount } else { 0.0d }

    return (avgScore, posCount, negCount, neutralCount)
  }


}
