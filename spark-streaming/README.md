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

1. Now that we're ready to code, we first need to make sure all we have all the necessary imports. Fortunately, for this simple example, we only need a few.

		```
		// Needed for all Spark jobs
		import org.apache.spark.SparkConf
		import org.apache.spark.SparkContext._

		// Only needed for Spark Streaming
		import org.apache.spark.streaming.Seconds
		import org.apache.spark.streaming.StreamingContext
		import org.apache.spark.streaming.StreamingContext._

		// Only needed for utilities for streaming from Twitter
		import org.apache.spark.streaming.twitter._
		```

1. In our main function, we can now configure Spark's streaming resources. The Spark config is straightforward, but creating the StreamingContext object takes an additional parameter called the _batch interval_. This interval should be set such that your cluster can process the data in less time than the interval, and it can have big implications for the rest of your system if set too low. Additionally, the batch interval provides a lower bound on how granular your real-time computation can be (e.g., no faster than every 5 seconds). We use 5 here as a conservative estimate. See Spark's [Performance Tuning](https://spark.apache.org/docs/latest/streaming-programming-guide.html#setting-the-right-batch-interval) for more information.

		```
		// Set up the Spark configuration with our app name and any other config
		//  parameters you want (e.g., Kryo serialization or executor memory)
		val sparkConf = new SparkConf().setAppName("TopHashtags")

		// Use the config to create a streaming context that creates a new RDD 
		//  with a batch interval of every 5 seconds. 
		val ssc = new StreamingContext(sparkConf, Seconds(5))
		```

1. We then use the Spark streaming context to create our Twitter stream (though we won't connect to it just yet). Spark is nice in giving us easy utility functions to create this stream. The `None` object refers to authentication information, which we leave blank to force Twitter4j's default authentication (i.e., use the `twitter4j.properties` file). The `createStream()` function can also take a set of filter keywords as well, but we omit that here. For more information, see the [TwitterUtils API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.twitter.TwitterUtils$).

		```
		// Use the streaming context and the TwitterUtils to create the 
		//  Twitter stream
		val stream = TwitterUtils.createStream(ssc, None)
		```

1. Our next goal is to define the operations on the batches of tweets Spark Streaming gives us. Since we want to count hashtags, our first step is to extract hashtags from each tweet.

		```
		// Each tweet comes as a twitter4j.Status object, which we can use to 
		//  extract hash tags. We use flatMap() since each status could have
		//  ZERO OR MORE hashtags
		val hashTags = stream.flatMap(status => status.getHashtagEntities)
		```

1. We then map each twitter4j.HashtagEntity object to a pair containing the "#" symbol, the text of the hashtag, and 1, which we will use in our reduction for counting.

		```
		// Convert hashtag to (hashtag, 1) pair for future reduction
		val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))
		```

1. To count the occurrences for a given hashtag pair, we would use reduceByKey(), but since we are using Spark streaming, we can also make use of its *sliding window* capabilities and instead use reduceByKeyAndWindow(). This function also takes a time parameter to control how many previous RDDs it uses when performing its reduceByKey operation. In this case, we're only looking at the past 10 seconds of RDDs, and since our batch interval is 5 seconds, we're only looking at the past two RDDs.

		```
		// use reduceByKeyAndWindow to reduce our hashtag pairs by summing their 
		//  counts over the last 10 seconds of batch intervals (in this case, 2 RDDs)
		val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(10))
		```

1. The topCounts10 variable will now point to a stream of RDDs, where each RDD is contains pairs of (hashtag, count). We want to sort each of these RDDs by the hashtag counts, so we call transform(), which applies an arbitrary function to each RDD.

		```
		// topCounts10 will provide a new RDD for every window. Calling transform()
		//  on each of these RDDs gives us a per-window transformation. We use
		//  this transformation to sort each RDD by the hashtag counts. The FALSE 
		//  flag tells the sortBy() function to sort in descending order
		val sortedTopCounts10 = topCounts10.transform(rdd => 
		  rdd.sortBy(hashtagPair => hashtagPair._2, false))
		```

1. For each of these sorted RDDs, we want to print the top 10 most popular hashtags. The foreachRDD() function makes this step easy, and we simply take the first 10 pairs from the RDD using take(10) and print them out.

		```
		// Print popular hashtags
		sortedTopCounts10.foreachRDD(rdd => {
		  val topList = rdd.take(10)
		  println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
		  topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
		})
		```

1. Once we've set up all the operations for the data we stream in, it's finally time to start up the stream. 

		```
		// Finally, start the streaming operation and continue until killed
		ssc.start()
		ssc.awaitTermination()
		```


## Building and Running Your Code

The last steps are to build and run the code.

1. To build, simply run `mvn clean package` in this directory

1. To run your code, use `spark-submit --class edu.umd.cs.hcil.sparkstreamingtwitterdemo.TopHashtags --master yarn-client ./target/SparkStreamingTwitterDemo-1.0-SNAPSHOT-jar-with-dependencies.jar`

__NOTE__: Be sure to update your `twitter4j.properties` file and have it in the directory where you run `spark-submit`.
