/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.{FileWriter, File}
import java.text.SimpleDateFormat
import java.util.{Locale, Calendar, Date}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import twitter4j.Status

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import twitter4j.json.DataObjectFactory
import org.geotools.data.shapefile.ShapefileDataStore
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.MultiPolygon

object DailyUSStateCounter {

  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"

  def UNKNOWN_STATE = "_UNKNOWN"

  /**
   * This code shows how to count the number of geocoded tweets in each state
   * using OpenGIS and shape files
   *
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StateCounter")
    val sc = new SparkContext(conf)
    
    // Tweets to read
    val dataPath = args(0)
    
    // Shape file with shapes for the US states
    //  I used the shape file from the Matplotlib examples on github
    //  URL: https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shp
    val shapePath = args(1)

    // Output file
    val outputPath = args(2)
    
    // Create a list of polygons that correspond to the US States
    //  We'll use these for testing whether a state contains a given GPS point
    val statePolyList = generateStatePolyList(shapePath)
    val stateNames = statePolyList.map(t => t._1).distinct :+ UNKNOWN_STATE
    
    // Broadcast that polygon list for quick access on the cluster nodes
    val broad_StatePolyList = sc.broadcast(statePolyList)
    
    // Read the tweets we'll process
    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)
    
    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size
    
    // Convert each JSON line in the file to a status using Twitter4j
    //  Note that not all lines are Status lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.'
    val tweets = twitterMsgs.map(line => {
        try {
          DataObjectFactory.createStatus(line)
        } catch {
          case e : Exception => null
        }
      })
    
    // Only keep non-null status with text
    val tweetsFiltered = tweets.filter(status => {       
        status != null &&
        status.getText != null &&
        status.getText.size > 0
      })

    // Filter again to keep tweets with a geo feature
    val geoTweets = tweetsFiltered.filter(status => {
      status.getGeoLocation != null ||
      status.getPlace != null
    })

    val timedTweets = geoTweets.map(status => (convertTimeToSlice(status.getCreatedAt), status))
    
    // For each geocoded tweet, pull its lat/lon and test whether it is in
    //  a US state. If so, map to a tuple (state name, 1) for grouping and 
    //  counting
    val datedTweetsToStates : RDD[(Date, (String, Int))] = timedTweets.flatMap(tuple => {
      val tweetTime = tuple._1
      val status = tuple._2

      var gpsList : List[(Date, (String, Int))] = List.empty
      val locations : List[(Double, Double)] = try {
        getGpsLocation(status)
      } catch {
        case e : Exception => {
          println("Failed on tweet: " + status.getId + ", " + e.getMessage + ", " + e.getStackTraceString)
          List.empty
        }
      }
//      val locations : List[(Double, Double)] = getGpsLocation(status)

      for ( e <- locations ) {
        val lat = e._1
        val lon = e._2

        if ( lat != 0 && lon != 0 ) {

          // Create a test point from the tweet's lat/lon
          val geometer = new GeometryFactory()
          val testCoordinate = new Coordinate(lon, lat)
          val testGeo = geometer.createPoint(testCoordinate)

          // Get the polygon list from the broadcast value
          val polyList = broad_StatePolyList.value

          var stateName: String = null

          // Index for searching the polygon list
          var index = 0

          // For each polygon and whle we have not found a state name, test if the
          //  test point is in the corresponding state's polygon
          while (index < polyList.size && stateName == null) {
            val polyTuple = polyList(index)
            val thisState = polyTuple._1
            val polygon = polyTuple._2

            if (polygon.contains(testGeo)) {
              stateName = thisState
            }

            // Advance the list index
            index = index + 1
          }

          // Special key for non-US tweets
          if (stateName == null) {
            stateName = UNKNOWN_STATE
          }

          gpsList = gpsList :+(tweetTime, (stateName, 1))
        }
      }

      gpsList
    })

    // Pull out just the times
    val times = timedTweets.map(tuple => tuple._1)
    val timeBounds = times.aggregate((new Date(Long.MaxValue), new Date(Long.MinValue)))((u, t) => {
      var min = u._1
      var max = u._2

      if ( t.before(min) ) {
        min = t
      }

      if ( t.after(max) ) {
        max = t
      }

      (min, max)
    },
      (u1, u2) => {
        var min = u1._1
        var max = u1._2

        if ( u2._1.before(min) ) {
          min = u2._1
        }

        if ( u2._2.after(max) ) {
          max = u2._2
        }

        (min, max)
      })
    val minTime = timeBounds._1
    val maxTime = timeBounds._2
    printf("Min Time: " + minTime + "\n")
    printf("Max Time: " + maxTime + "\n")

    val timeList = constructDateList(minTime, maxTime)

    // List of states for each day
    var dateTokenMap : Map[Date, List[Int]] = Map.empty
    for ( time <- timeList ) {

      val thisTimesStates = datedTweetsToStates.filter(tuple => tuple._1.compareTo(time) == 0).map(tuple => tuple._2)
      val stateCountMap = thisTimesStates.reduceByKey((l, r) => l + r).collectAsMap()

      val stateCounts : List[Int] = stateNames.map(n => stateCountMap.getOrElse(n, 0))

      dateTokenMap = dateTokenMap + (time -> stateCounts)
    }

    val outputFileWriter : FileWriter = new FileWriter(outputPath)
    val writer = new CSVPrinter(outputFileWriter, CSVFormat.DEFAULT)

    writer.printRecord((List("") ++ stateNames).asJava)

    for ( time <- timeList ) {
      val dateString : String = dateFormatter(time)
      val elements : List[String] = List(dateString) ++ dateTokenMap(time).map(c => c.toString)

      writer.printRecord(elements.asJava)
    }
    writer.flush()
    outputFileWriter.close()
  }

  /**
   * Given the path to a shape file, generate a list of tuples containing state
   *  names and polygons. This list will likely have multiple polygons per state
   *
   * @param shapePath the path to the shape file to use
   */
  def generateStatePolyList(shapePath : String) : List[Tuple2[String,MultiPolygon]] = {

    var polyList : List[Tuple2[String,MultiPolygon]] = List.empty
    
    val file = new File(shapePath);
    val dataStore = new ShapefileDataStore(file.toURI().toURL())
    
    val reader = dataStore.getFeatureReader()
    while ( reader.hasNext ) {
      val ftr = reader.next
      val stateName : String = ftr.getAttribute("NAME").asInstanceOf[String]
      val polygon : MultiPolygon = ftr.getDefaultGeometry().asInstanceOf[MultiPolygon]

      polyList = polyList :+ (stateName, polygon)
    }
    
    return polyList
  }

  /**
   * Flatten timestamp to the appropriate scale
   *
   * @param time The date to flatten
   */
  def convertTimeToSlice(time : Date) : Date = {
    val cal = Calendar.getInstance
    cal.setTime(time)

    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 0)

    return cal.getTime
  }

  /**
   * Build the full date list between start and end
   *
   * @param startDate The first date in the list
   * @param endDate The last date in the list
   */
  def constructDateList(startDate : Date, endDate : Date) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)

    var l = List[Date]()

    while(cal.getTime.before(endDate)) {
      l = l :+ cal.getTime

      cal.add(Calendar.DATE, 1)
    }
    l = l :+ endDate

    return l
  }

  /**
   * Convert a given date into a string using TIME_FORMAT
   *
   * @param date The date to convert
   */
  def dateFormatter(date : Date) : String = {
    val sdf = new SimpleDateFormat(TIME_FORMAT, Locale.US);
    sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));

    sdf.format(date)
  }

  /**
   * Extract GPS coordinates from a tweet
   *
   * @param status The tweet from which we'll extract the GPS
   */
  def getGpsLocation(status : Status) : List[(Double, Double)] = {
    //    # Hack function for reducing a bounding box to a single GPS lat/lon pair

    var locList: List[(Double, Double)] = List.empty

    if (status.getGeoLocation != null) {
      val lat = status.getGeoLocation.getLatitude
      val lon = status.getGeoLocation.getLongitude

      locList = locList :+ (lat, lon)
    } else if (status.getPlace != null && status.getPlace.getBoundingBoxCoordinates != null) {

      for (geos <- status.getPlace.getBoundingBoxCoordinates) {
        var lon: Double = 0
        var lat: Double = 0

        // Array[Array[GeoLocation]]
        val latList = geos.map(g => g.getLatitude)
        val lonList = geos.map(g => g.getLongitude)

        lat = latList.foldLeft(0d)((a, b) => a + b) / latList.size.toDouble
        lon = lonList.foldLeft(0d)((a, b) => a + b) / lonList.size.toDouble

        locList = locList :+ (lat, lon)
      }
    }

    return locList
  }
}
