package edu.umd.cs.hcil.spark.analytics

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

import java.io.File

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import twitter4j.Status
import org.geotools.data.shapefile.ShapefileDataStore
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.MultiPolygon
import edu.umd.cs.hcil.spark.analytics.utils.JsonUtils

object GeoExtractorByName {

  /**
    * Save only those GPS-coded tweets from a given country or named area
    *
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Geo Extractor by Name")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val shapePath = args(1)
    val outputPath = args(2)
    val targetState = args(3)

    val statePolyList = generateStatePolyList(shapePath, targetState)

    if ( statePolyList.size == 0 ) {
      println("No state shapes matching target: " + targetState)
      exit(1)
    }

    val broad_StatePolyList = sc.broadcast(statePolyList)

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 4 ) {
      val initialPartitions = args(4).toInt
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
        (line, JsonUtils.jsonToStatus(line))
      } catch {
        case e : Exception => (line, null)
      }
    }).filter(tuple => tuple._2 != null)

    // Filter to keep tweets with a geo feature
    val geoTweets = tweets.filter(tuple => {
      val status = tuple._2

      status.getGeoLocation != null
    })

    // Filter based on whether the tweet is in the given location or not
    val tweetsToStates : RDD[Tuple2[String,Status]] = geoTweets.filter(tuple => {
      val status = tuple._2

      val geoLoc = status.getGeoLocation
      val lat = geoLoc.getLatitude
      val lon = geoLoc.getLongitude

      val geometer = new GeometryFactory()
      val testCoordinate = new Coordinate(lon, lat)
      val testGeo = geometer.createPoint(testCoordinate)

      val polyList = broad_StatePolyList.value
      var foundFlag : Boolean = false
      var index = 0

      while ( index < polyList.size && foundFlag != true ) {
        val polyTuple = polyList(index)
        val polygon = polyTuple._2

        if ( polygon.contains(testGeo) ) {
          foundFlag = true
        }

        index = index + 1
      }

      foundFlag
    })

    tweetsToStates.map(tuple => {tuple._1}).repartition(newPartitionSize)
      .saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  /**
    * Given the path to a shape file, generate a list of tuples containing state
    *  names and polygons. This list will likely have multiple polygons per state
    *
    * @param shapePath the path to the shape file to use
    */
  def generateStatePolyList(shapePath : String, targetState : String) : List[Tuple2[String,MultiPolygon]] = {

    var polyList : List[Tuple2[String,MultiPolygon]] = List.empty

    val file = new File(shapePath);
    val dataStore = new ShapefileDataStore(file.toURI().toURL())

    val reader = dataStore.getFeatureReader()
    while ( reader.hasNext ) {
      val ftr = reader.next
      val stateName : String = ftr.getAttribute("NAME").asInstanceOf[String]

      if ( stateName.compareToIgnoreCase(targetState) == 0 ) {
        val polygon : MultiPolygon = ftr.getDefaultGeometry().asInstanceOf[MultiPolygon]

        polyList = polyList :+ (stateName, polygon)
      }
    }

    return polyList
  }
}
