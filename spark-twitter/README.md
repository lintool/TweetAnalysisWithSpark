# Twitter Analytics with Spark Streaming

This project is a quick tutorial to show how to use Spark to analyze Twitter data stored in gzipped archives.
For this exercise, we will demonstrate counting the number of tweets contained in our archive per minute, per hour, or per day.

The code we'll be using is written in Scala, should be compiled with Maven, and submitted to your cluster (or Spark standalone installation) using spark-submit.
The code we will reference here can be executed in `spark-shell` as well.


## Setting Up the Environment

1. For simplicity, include the Spark Streaming library for Twitter, called `spark-streaming-twitter_2.10`, which will be included in our `pom.xml` file as a dependency (along with `spark-core_2.10` and `spark-streaming_2.10`). This dependency is really only for Twitter4j, which we need to convert raw JSON into Twitter's Status object.


## Building and Running Your Code

The last steps are to build and run the code.

1. To build, simply run `mvn clean package` in this directory.

1. To run your code:

		```
		$ spark-submit --class TweetFrequency --master yarn-client \
		   ./target/SparkTwitterDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
		   <-m|-h|-d> <input_file> <output_dir> [numPartitions]
		```

1. Note that you might need to set up the environment variable due to [this issue](https://issues.cloudera.org/browse/DISTRO-664):

		```
		$ export HADOOP_CONF_DIR=/etc/hadoop/conf
		```

1. After running, you should have a directory <output_dir> containing part files with time stamps and the counts of tweets in that time stamp.
