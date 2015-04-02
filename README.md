# Tweet Analysis with Spark

This tutorial shows how to use Spark to analyze crawls from Twitter's streaming API, which is formatted as a sequence of JSON objects, one per line. We assume CDH 5.3.0.

Fire up the Spark shell:

```
$ export HADOOP_CONF_DIR=/etc/hadoop/conf
$ spark-shell --master yarn-client --jars gson-2.3.1.jar --num-executors 10
```

The need for setting up the environment variable is due to [this issue](https://issues.cloudera.org/browse/DISTRO-664). You can download the jar [here](http://search.maven.org/#artifactdetails%7Ccom.google.code.gson%7Cgson%7C2.3.1%7Cjar).

If you want to play with individual tweets:

```
import com.google.gson._

val jsonParser = new JsonParser()
val gson = new GsonBuilder().setPrettyPrinting().create()

val raw = sc.textFile("/shared/collections/twitter-2015-03-test-crawl/east/statuses.log.2015-03-01-00.gz");

// Take five JSON records and print each out.
for (r <- raw.take(5)) {
  println(gson.toJson(jsonParser.parse(r)))
}

```

Now let's analyze the tweets at scale. Here's how we start:

```
import com.google.gson._

val raw = sc.textFile("/shared/collections/twitter-2015-03-test-crawl/east/statuses.log.2015-03-01-00.gz");

var tweets =
  raw.map(line => {
    val jsonParser = new JsonParser()
    val obj = jsonParser.parse(line).getAsJsonObject()
    if (obj.has("delete")) {
      null
    } else {
      (obj.get("id").getAsLong(), obj.get("text").getAsString())
    }
  }).filter(x => x != null)
```

The variable `tweets` now holds an RDD of (tweetid, tweet text) pairs, with deletes filtered out.

Let's continue with word count:

```
val wc =
  tweets.flatMap(t => t._2.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
```

Let's say, find common terms, sort them, and save output to disk:

```
wc.filter(t => t._2 > 100).sortByKey().saveAsTextFile("wc_out")
```
