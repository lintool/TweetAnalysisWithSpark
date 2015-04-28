# Real-Time Twitter Analytics with Spark Streaming

This project is a quick tutorial to show how to use Spark's streaming capabilities to analyze Twitter data directly from Twitter's [sample stream](https://dev.twitter.com/streaming/public).
As an exercise, we will demonstrate how to extract the most popular hashtags in Twitter's sample stream over the past few minutes.

The code we'll be using is written in Scala, should be compiled with Maven, and submitted to your cluster (or Spark standalone installation) using spark-submit.
The code we will reference here can be executed in `spark-shell` as well.


## Setting Up the Environment

1. The first thing to note is that you'll need a set of credentials from Twitter's Developer website to authenticate against Twitter's servers and access the sample stream. You can get these credentials from Twitter's [Application Management](https://apps.twitter.com/) site.

1. Spark Streaming includes a special library for Twitter access, called `spark-streaming-twitter_2.10`, which will be included in our `pom.xml` file as a dependency (along with `spark-core_2.10` and `spark-streaming_2.10`).

1. The Spark Streaming Twitter library makes use of [Twitter4j](http://twitter4j.org/) 3.0.3 for handling authentication and handshaking with Twitter's streaming API. As a result, we will need to populate a `twitter4j.properties` file with the oauth.consumerKey, oauth.consumerSecret, oauth.accessToken, and oauth.accessTokenSecret values we got from Twitter's app management site. For simplicity, this properties file should be in the current directory from which you launch your Spark application.


## Coding For Streams

All code referenced below appears in `edu.umd.cs.hcil.sparkstreamingtwitterdemo.TwitterStreamer` in the `src` directory.

1. Now that we're ready to code, we first need to make sure all we have all the necessary imports. Fortunately, for this simple example, we only need 4.

```
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
```









