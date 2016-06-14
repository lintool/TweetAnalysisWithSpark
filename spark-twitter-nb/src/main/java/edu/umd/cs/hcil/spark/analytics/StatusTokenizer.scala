package edu.umd.cs.hcil.spark.analytics

/**
 * Created by cbuntain on 5/31/16.
 */
import twitter4j._
import java.util.regex.Pattern
import scala.io.Source

object StatusTokenizer {

  val punctRegex = Pattern.compile("[\\p{Punct}\n\t]")

  def tokenize(status : Status) : List[String] = {

    var tweetText = status.getText
    var tokens : List[String] = List.empty

    // Replace hashtags with space
    for ( hashtagEntity <- status.getHashtagEntities ) {
      val hashtag = "#" + hashtagEntity.getText
      tokens = tokens :+ hashtag

      tweetText = tweetText.replace(hashtag, " ")
    }

    // Replace mentions
    for ( mentionEntity <- status.getUserMentionEntities ) {
      val mention = "@" + mentionEntity.getScreenName
      tokens = tokens :+ mention

      tweetText = tweetText.replace(mention, " ")
    }

    // Replace URLs
    for ( urlEntity <- status.getURLEntities ) {
      val url = urlEntity.getExpandedURL
      tokens = tokens :+ url
      println(urlEntity.getURL)

      tweetText = tweetText.replace(urlEntity.getURL, " ")
    }

    // Replace media with URLs
    for ( mediaEntity <- status.getMediaEntities ) {
      val mediaUrl = mediaEntity.getExpandedURL
      tokens = tokens :+ mediaUrl

      tweetText = tweetText.replace(mediaEntity.getURL, " ")
    }

    val cleanedTweetText = punctRegex.matcher(tweetText).replaceAll(" ")
    tokens = tokens ++ cleanedTweetText.split(" ").filter(token => token.length > 0)

    return tokens
  }

  def main(args: Array[String]): Unit = {
    println("Reading tweets from: " + args(0))

    val filename = args(0)

    for (line <- Source.fromFile(filename).getLines()) {

      val status = TwitterObjectFactory.createStatus(line)
      println(status.getText)
      for (token <- tokenize(status)) {
        println("[" + token + "]")
      }
    }
  }
}